package solana

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/catalog/quicknode"
)

var (
	quicknodeSolanaMainnetHTTPURL = monitord.RequiredSecret("quicknode", "solana-mainnet-http-url")
	quicknodeSolanaMainnetWSSURL  = monitord.RequiredSecret("quicknode", "solana-mainnet-websocket-url")
)

const (
	addressEventsCursorSource = "quicknode.solana.address-events-cursor"
	addressEventsLiveSource   = "quicknode.solana.address-events-live"
	maxAddressLiveEvents      = 256
)

type AddressEvent struct {
	Signature   Signature
	Slot        Slot
	Err         json.RawMessage
	Memo        *string
	BlockTime   *int64
	Transaction json.RawMessage
}

func (e AddressEvent) ID() string { return "solana:transaction:" + string(e.Signature) }

type AddressEventUpdate[S any] func(*monitord.Tx[S]) error

type addressEventsCursor struct {
	GenesisHash      GenesisHash `json:"genesis_hash"`
	Address          PublicKey   `json:"address"`
	Signature        Signature   `json:"signature,omitempty"`
	Slot             Slot        `json:"slot,omitempty"`
	TransactionIndex uint64      `json:"transaction_index,omitempty"`
}

type addressEventsLiveJournal struct {
	Signatures []Signature `json:"signatures,omitempty"`
}

type addressEventHint struct {
	Matched bool
	Handled bool
	Event   AddressEvent
}

// AddressEvents processes transactions involving one Solana address. QuickNode
// transactionSubscribe is the live source; finalized HTTP history repairs gaps.
type AddressEvents[S any] struct {
	Name                string
	Description         string
	Address             PublicKey
	ExpectedGenesisHash GenesisHash
	HTTPURL             string
	WSSURL              string
	HTTPSecret          monitord.SecretRef
	WSSSecret           monitord.SecretRef
	BackfillFrom        *Slot
	PollInterval        time.Duration
	LiveCommitment      Commitment
	MatchLogs           func(LogsValue) bool
	Handle              func(context.Context, *Client, AddressEvent) (AddressEventUpdate[S], error)
}

var _ monitord.Monitor[struct{}] = AddressEvents[struct{}]{}

func (e AddressEvents[S]) Info() monitord.Info {
	name := e.Name
	if name == "" {
		name = "quicknode-solana-address-events"
	}
	description := e.Description
	if description == "" {
		description = "Live Solana transactions with finalized HTTP recovery"
	}
	return monitord.Info{Name: name, Description: description}
}

func (e AddressEvents[S]) Plan() monitord.Plan[S] {
	refs := []monitord.SecretRef{}
	if e.HTTPURL == "" {
		ref, _ := e.httpSecret()
		refs = append(refs, ref)
	}
	if e.WSSURL == "" {
		if secretConfigured(e.WSSSecret) {
			ref := e.WSSSecret
			ref.Required = true
			refs = append(refs, ref)
		} else if _, defaultEndpoint := e.httpSecret(); e.HTTPURL == "" && defaultEndpoint {
			refs = append(refs, quicknodeSolanaMainnetWSSURL)
		}
	}
	return monitord.Continuous(e.run, monitord.WithSecrets(refs...))
}

func (e AddressEvents[S]) run(ctx context.Context, session *monitord.Session[S]) error {
	if e.Handle == nil {
		return errors.New("quicknode solana: AddressEvents.Handle is required")
	}
	if err := e.Address.Validate(); err != nil {
		return err
	}
	httpURL := e.HTTPURL
	wssURL := e.WSSURL
	httpRef, defaultSecret := e.httpSecret()
	defaultEndpoint := e.HTTPURL == "" && defaultSecret
	if httpURL == "" {
		var err error
		httpURL, err = session.Secrets().Require(httpRef)
		if err != nil {
			return err
		}
	}
	if wssURL == "" {
		wssRef := e.WSSSecret
		if !secretConfigured(wssRef) && defaultEndpoint {
			wssRef = quicknodeSolanaMainnetWSSURL
		}
		if !secretConfigured(wssRef) {
			return errors.New("quicknode solana: AddressEvents requires WSSURL or WSSSecret")
		}
		var err error
		wssURL, err = session.Secrets().Require(wssRef)
		if err != nil {
			return err
		}
	}
	expectedGenesisHash := e.ExpectedGenesisHash
	if expectedGenesisHash == "" && defaultEndpoint {
		expectedGenesisHash = MainnetGenesisHash
	}
	client, err := Open(ctx, Config{
		Endpoint:            quicknode.Endpoint{HTTPURL: httpURL, WSSURL: wssURL},
		Commitment:          Finalized,
		ExpectedGenesisHash: expectedGenesisHash,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	liveCommitment := e.LiveCommitment
	if liveCommitment == "" {
		liveCommitment = Confirmed
	}
	subscription, err := client.SubscribeTransactions(ctx, TransactionFilter{
		ExcludeVotes:  true,
		ExcludeFailed: true,
		Accounts:      TransactionAccountsFilter{Include: []PublicKey{e.Address}},
	}, liveCommitment)
	if err != nil {
		return err
	}
	defer subscription.Close()

	cursor, err := e.loadCursor(ctx, session, client)
	if err != nil {
		return err
	}
	journal, err := e.loadLiveJournal(session)
	if err != nil {
		return err
	}
	pollInterval := e.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	hints := map[Signature]addressEventHint{}
	if err := e.catchUp(ctx, session, client, &cursor, &journal, hints); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.catchUp(ctx, session, client, &cursor, &journal, hints); err != nil {
				return err
			}
		case notification, ok := <-subscription.C():
			if !ok {
				return errors.New("quicknode solana: transaction subscription closed")
			}
			if err := e.handleLive(ctx, session, client, cursor, &journal, hints, notification); err != nil {
				return err
			}
		case err, ok := <-subscription.Err():
			if ok && err != nil {
				return err
			}
			return errors.New("quicknode solana: transaction subscription stopped")
		}
	}
}

func (e AddressEvents[S]) httpSecret() (monitord.SecretRef, bool) {
	if !secretConfigured(e.HTTPSecret) {
		return quicknodeSolanaMainnetHTTPURL, true
	}
	return e.HTTPSecret, false
}

func secretConfigured(ref monitord.SecretRef) bool {
	return ref.Group != "" || ref.Key != ""
}

func (e AddressEvents[S]) loadLiveJournal(session *monitord.Session[S]) (addressEventsLiveJournal, error) {
	var journal addressEventsLiveJournal
	_, err := session.Checkpoint(addressEventsLiveSource, &journal)
	if err != nil {
		return journal, err
	}
	if len(journal.Signatures) > maxAddressLiveEvents {
		return journal, errors.New("quicknode solana: live event journal exceeds its bound")
	}
	return journal, nil
}

func (e AddressEvents[S]) handleLive(
	ctx context.Context,
	session *monitord.Session[S],
	client *Client,
	cursor addressEventsCursor,
	journal *addressEventsLiveJournal,
	hints map[Signature]addressEventHint,
	notification TransactionNotification,
) error {
	value := notification.Value
	if value.Signature == "" {
		return errors.New("quicknode solana: transaction notification omitted its signature")
	}
	if value.Slot == 0 {
		value.Slot = notification.Context.Slot
	}
	if value.Signature == cursor.Signature || liveSignatureIndex(journal.Signatures, value.Signature) >= 0 {
		return nil
	}
	event := AddressEvent{
		Signature:   value.Signature,
		Slot:        value.Slot,
		Err:         append(json.RawMessage(nil), value.Err...),
		Transaction: append(json.RawMessage(nil), value.Transaction...),
	}
	matched, err := e.matchesTransaction(event)
	if err != nil {
		return err
	}
	hint := addressEventHint{Matched: matched, Event: event}
	if !matched || len(journal.Signatures) >= maxAddressLiveEvents {
		hints[value.Signature] = hint
		return nil
	}
	update, err := e.Handle(ctx, client, event)
	if err != nil {
		return err
	}
	next := cloneAddressLiveJournal(*journal)
	next.Signatures = append(next.Signatures, value.Signature)
	if err := session.Commit(ctx, func(tx *monitord.Tx[S]) error {
		if update != nil {
			if err := update(tx); err != nil {
				return err
			}
		}
		return tx.Checkpoint(addressEventsLiveSource, next)
	}); err != nil {
		return err
	}
	hint.Handled = true
	hints[value.Signature] = hint
	*journal = next
	return nil
}

func (e AddressEvents[S]) matchesTransaction(event AddressEvent) (bool, error) {
	if e.MatchLogs == nil {
		return true, nil
	}
	var record struct {
		Meta struct {
			LogMessages []string        `json:"logMessages"`
			Err         json.RawMessage `json:"err"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(event.Transaction, &record); err != nil {
		return false, fmt.Errorf("quicknode solana: decode subscribed transaction logs: %w", err)
	}
	return e.MatchLogs(LogsValue{
		Signature: event.Signature,
		Err:       append(json.RawMessage(nil), record.Meta.Err...),
		Logs:      append([]string(nil), record.Meta.LogMessages...),
	}), nil
}

func liveSignatureIndex(signatures []Signature, target Signature) int {
	for i, signature := range signatures {
		if signature == target {
			return i
		}
	}
	return -1
}

func cloneAddressLiveJournal(journal addressEventsLiveJournal) addressEventsLiveJournal {
	return addressEventsLiveJournal{Signatures: append([]Signature(nil), journal.Signatures...)}
}

func (e AddressEvents[S]) loadCursor(ctx context.Context, session *monitord.Session[S], client *Client) (addressEventsCursor, error) {
	var cursor addressEventsCursor
	found, err := session.Checkpoint(addressEventsCursorSource, &cursor)
	if err != nil {
		return cursor, err
	}
	if found {
		if cursor.GenesisHash != client.GenesisHash() {
			return cursor, fmt.Errorf("quicknode solana: cursor genesis hash is %s, endpoint is %s", cursor.GenesisHash, client.GenesisHash())
		}
		if cursor.Address != e.Address {
			return cursor, fmt.Errorf("quicknode solana: cursor address is %s, monitor address is %s", cursor.Address, e.Address)
		}
		return cursor, nil
	}
	cursor = addressEventsCursor{GenesisHash: client.GenesisHash(), Address: e.Address}
	if e.BackfillFrom != nil {
		if *e.BackfillFrom > 0 {
			cursor.Slot = *e.BackfillFrom - 1
		}
	} else {
		latest, err := client.GetTransactionsForAddress(ctx, e.Address, AddressTransactionsOptions{
			Commitment: Finalized,
			Limit:      1,
		})
		if err != nil {
			return cursor, err
		}
		if len(latest.Data) > 0 {
			cursor.Signature, err = latest.Data[0].Signature()
			if err != nil {
				return cursor, err
			}
			cursor.Slot = latest.Data[0].Slot
			cursor.TransactionIndex = latest.Data[0].TransactionIndex
		} else {
			cursor.Slot, err = client.GetSlot(ctx)
			if err != nil {
				return cursor, err
			}
			cursor.TransactionIndex = ^uint64(0)
		}
	}
	if err := session.Commit(ctx, func(tx *monitord.Tx[S]) error {
		return tx.Checkpoint(addressEventsCursorSource, cursor)
	}); err != nil {
		return cursor, err
	}
	return cursor, nil
}

func (e AddressEvents[S]) catchUp(
	ctx context.Context,
	session *monitord.Session[S],
	client *Client,
	cursor *addressEventsCursor,
	journal *addressEventsLiveJournal,
	hints map[Signature]addressEventHint,
) error {
	const pageSize = 100
	fromSlot := cursor.Slot
	pageToken := ""
	for {
		page, err := client.GetTransactionsForAddress(ctx, e.Address, AddressTransactionsOptions{
			Commitment:      Finalized,
			FromSlot:        fromSlot,
			PaginationToken: pageToken,
			Limit:           pageSize,
			Ascending:       true,
		})
		if err != nil {
			return err
		}
		dirtyCursor := false
		for _, record := range page.Data {
			if record.Slot < cursor.Slot || record.Slot == cursor.Slot && record.TransactionIndex <= cursor.TransactionIndex {
				continue
			}
			signature, err := record.Signature()
			if err != nil {
				return err
			}
			hint, hinted := hints[signature]
			delete(hints, signature)
			nextJournal := cloneAddressLiveJournal(*journal)
			journalIndex := liveSignatureIndex(nextJournal.Signatures, signature)
			var update AddressEventUpdate[S]
			mustCommit := journalIndex >= 0
			if journalIndex >= 0 {
				nextJournal.Signatures = append(nextJournal.Signatures[:journalIndex], nextJournal.Signatures[journalIndex+1:]...)
			} else if hinted && hint.Matched && !hint.Handled {
				update, err = e.Handle(ctx, client, hint.Event)
			} else if !hinted {
				payload, payloadErr := record.Payload()
				if payloadErr != nil {
					return payloadErr
				}
				event := AddressEvent{
					Signature:   signature,
					Slot:        record.Slot,
					Err:         append(json.RawMessage(nil), record.Err...),
					Memo:        record.Memo,
					BlockTime:   record.BlockTime,
					Transaction: payload,
				}
				matched, matchErr := e.matchesTransaction(event)
				if matchErr != nil {
					return matchErr
				}
				if matched {
					update, err = e.Handle(ctx, client, event)
				}
			}
			if err != nil {
				return err
			}
			next := addressEventsCursor{
				GenesisHash:      client.GenesisHash(),
				Address:          e.Address,
				Signature:        signature,
				Slot:             record.Slot,
				TransactionIndex: record.TransactionIndex,
			}
			if update != nil || mustCommit {
				if err := commitAddressProgress(ctx, session, next, nextJournal, update); err != nil {
					return err
				}
				*journal = nextJournal
				dirtyCursor = false
			} else {
				dirtyCursor = true
			}
			*cursor = next
		}
		if dirtyCursor {
			if err := commitAddressProgress(ctx, session, *cursor, *journal, nil); err != nil {
				return err
			}
		}
		if len(page.Data) < pageSize || page.PaginationToken == "" {
			return nil
		}
		if page.PaginationToken == pageToken {
			return errors.New("quicknode solana: address history pagination did not advance")
		}
		pageToken = page.PaginationToken
	}
}

func commitAddressProgress[S any](
	ctx context.Context,
	session *monitord.Session[S],
	cursor addressEventsCursor,
	journal addressEventsLiveJournal,
	update AddressEventUpdate[S],
) error {
	return session.Commit(ctx, func(tx *monitord.Tx[S]) error {
		if update != nil {
			if err := update(tx); err != nil {
				return err
			}
		}
		if err := tx.Checkpoint(addressEventsCursorSource, cursor); err != nil {
			return err
		}
		return tx.Checkpoint(addressEventsLiveSource, journal)
	})
}
