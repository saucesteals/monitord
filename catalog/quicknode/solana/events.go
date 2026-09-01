package solana

import (
	"bytes"
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
	GenesisHash GenesisHash `json:"genesis_hash"`
	Address     PublicKey   `json:"address"`
	Signature   Signature   `json:"signature,omitempty"`
	Slot        Slot        `json:"slot,omitempty"`
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
	Name                  string
	Description           string
	Address               PublicKey
	ExpectedGenesisHash   GenesisHash
	HTTPURL               string
	WSSURL                string
	HTTPSecret            monitord.SecretRef
	WSSSecret             monitord.SecretRef
	BackfillAfter         Signature
	PollInterval          time.Duration
	MaxBackfillPages      int
	ResumeFromLatestOnGap bool
	LiveCommitment        Commitment
	MatchLogs             func(LogsValue) bool
	Handle                func(context.Context, *Client, AddressEvent) (AddressEventUpdate[S], error)
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
	if e.BackfillAfter != "" {
		if err := e.BackfillAfter.Validate(); err != nil {
			return err
		}
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
	cursor = addressEventsCursor{GenesisHash: client.GenesisHash(), Address: e.Address, Signature: e.BackfillAfter}
	if cursor.Signature == "" {
		latest, err := client.GetSignaturesForAddress(ctx, e.Address, SignaturesOptions{Commitment: Finalized, Limit: 1})
		if err != nil {
			return cursor, err
		}
		if len(latest) > 0 {
			cursor.Signature = latest[0].Signature
			cursor.Slot = latest[0].Slot
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
	const pageSize = 1000
	maxPages := e.MaxBackfillPages
	if maxPages <= 0 {
		maxPages = 20
	}
	before := Signature("")
	seen := map[Signature]bool{}
	pending := []SignatureInfo{}
	var latest *SignatureInfo
	hasCursor := cursor.Signature != ""
	reachedCursor := false
	for page := 0; page < maxPages; page++ {
		batch, err := client.GetSignaturesForAddress(ctx, e.Address, SignaturesOptions{
			Commitment: Finalized,
			Before:     before,
			Limit:      pageSize,
		})
		if err != nil {
			return err
		}
		if page == 0 && len(batch) > 0 {
			value := batch[0]
			latest = &value
		}
		for _, info := range batch {
			if hasCursor && info.Signature == cursor.Signature {
				reachedCursor = true
				break
			}
			if !seen[info.Signature] {
				seen[info.Signature] = true
				pending = append(pending, info)
			}
		}
		if hasCursor && reachedCursor {
			break
		}
		if len(batch) < pageSize {
			if hasCursor {
				return e.historyGap(ctx, session, client, cursor, journal, hints, latest,
					fmt.Errorf("quicknode solana: saved signature %s is unavailable in address history", cursor.Signature))
			}
			reachedCursor = true
			break
		}
		if page+1 == maxPages {
			return e.historyGap(ctx, session, client, cursor, journal, hints, latest,
				fmt.Errorf("quicknode solana: address backfill exceeded %d pages", maxPages))
		}
		before = batch[len(batch)-1].Signature
	}
	if !reachedCursor {
		return fmt.Errorf("quicknode solana: saved signature %s was not reached within %d pages", cursor.Signature, maxPages)
	}
	dirtyCursor := false
	for i := len(pending) - 1; i >= 0; i-- {
		info := pending[i]
		hint, hinted := hints[info.Signature]
		delete(hints, info.Signature)
		nextJournal := cloneAddressLiveJournal(*journal)
		journalIndex := liveSignatureIndex(nextJournal.Signatures, info.Signature)
		var update AddressEventUpdate[S]
		mustCommit := journalIndex >= 0
		if journalIndex >= 0 {
			nextJournal.Signatures = append(nextJournal.Signatures[:journalIndex], nextJournal.Signatures[journalIndex+1:]...)
		} else if hinted && hint.Matched && !hint.Handled {
			var err error
			update, err = e.Handle(ctx, client, hint.Event)
			if err != nil {
				return err
			}
		} else if !hinted {
			payload, err := client.GetTransaction(ctx, info.Signature, TransactionOptions{Commitment: Finalized})
			if err != nil {
				return err
			}
			if bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
				return fmt.Errorf("quicknode solana: finalized transaction %s is temporarily unavailable", info.Signature)
			}
			event := AddressEvent{
				Signature:   info.Signature,
				Slot:        info.Slot,
				Err:         append(json.RawMessage(nil), info.Err...),
				Memo:        info.Memo,
				BlockTime:   info.BlockTime,
				Transaction: append(json.RawMessage(nil), payload...),
			}
			matched, err := e.matchesTransaction(event)
			if err != nil {
				return err
			}
			if matched {
				update, err = e.Handle(ctx, client, event)
			}
			if err != nil {
				return err
			}
		}
		next := addressEventsCursor{
			GenesisHash: client.GenesisHash(),
			Address:     e.Address,
			Signature:   info.Signature,
			Slot:        info.Slot,
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
		return commitAddressProgress(ctx, session, *cursor, *journal, nil)
	}
	return nil
}

func (e AddressEvents[S]) historyGap(
	ctx context.Context,
	session *monitord.Session[S],
	client *Client,
	cursor *addressEventsCursor,
	journal *addressEventsLiveJournal,
	hints map[Signature]addressEventHint,
	latest *SignatureInfo,
	cause error,
) error {
	if !e.ResumeFromLatestOnGap {
		return cause
	}
	next := addressEventsCursor{GenesisHash: client.GenesisHash(), Address: e.Address}
	if latest != nil {
		next.Signature = latest.Signature
		next.Slot = latest.Slot
	}
	if err := commitAddressProgress(ctx, session, next, *journal, nil); err != nil {
		return err
	}
	*cursor = next
	clear(hints)
	return nil
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
