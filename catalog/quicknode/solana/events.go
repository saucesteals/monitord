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
	quicknodeSolanaMainnetWSSURL  = monitord.OptionalSecret("quicknode", "solana-mainnet-websocket-url")
)

const addressEventsCursorSource = "quicknode.solana.address-events-cursor"

type AddressEvent struct {
	Signature   Signature
	Slot        Slot
	Err         json.RawMessage
	Memo        *string
	BlockTime   *int64
	Transaction json.RawMessage
}

func (e AddressEvent) ID() string { return "solana:transaction:" + string(e.Signature) }

type addressEventsCursor struct {
	GenesisHash GenesisHash `json:"genesis_hash"`
	Address     PublicKey   `json:"address"`
	Signature   Signature   `json:"signature,omitempty"`
	Slot        Slot        `json:"slot,omitempty"`
}

// AddressEvents processes finalized transactions involving one Solana address.
// HTTP history is authoritative; WSS notifications only reduce polling latency.
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
	MatchLogs             func(LogsValue) bool
	Handle                func(context.Context, *Client, *monitord.Tx[S], AddressEvent) error
}

var _ monitord.Monitor[struct{}] = AddressEvents[struct{}]{}

func (e AddressEvents[S]) Info() monitord.Info {
	name := e.Name
	if name == "" {
		name = "quicknode-solana-address-events"
	}
	description := e.Description
	if description == "" {
		description = "Finalized Solana transactions involving one address"
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
			refs = append(refs, e.WSSSecret)
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
		if secretConfigured(e.WSSSecret) {
			wssURL, _ = session.Secrets().Get(e.WSSSecret)
		} else if defaultEndpoint {
			wssURL, _ = session.Secrets().Get(quicknodeSolanaMainnetWSSURL)
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

	var notifications <-chan LogsNotification
	var subscriptionErrors <-chan error
	if wssURL != "" {
		subscription, err := client.SubscribeLogs(ctx, LogsFilter{Mentions: e.Address}, Finalized)
		if err == nil {
			defer subscription.Close()
			notifications = subscription.C()
			subscriptionErrors = subscription.Err()
		}
	}

	cursor, err := e.loadCursor(ctx, session, client)
	if err != nil {
		return err
	}
	pollInterval := e.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	logMatches := map[Signature]bool{}
	for {
		if err := e.catchUp(ctx, session, client, &cursor, logMatches); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case notification, ok := <-notifications:
			if !ok {
				notifications = nil
				continue
			}
			e.rememberLogMatch(logMatches, cursor, notification)
		case err, ok := <-subscriptionErrors:
			if !ok {
				subscriptionErrors = nil
				continue
			}
			if err != nil {
				notifications = nil
				subscriptionErrors = nil
			}
		}
	}
}

func (e AddressEvents[S]) rememberLogMatch(matches map[Signature]bool, cursor addressEventsCursor, notification LogsNotification) {
	if e.MatchLogs == nil || notification.Context.Slot <= cursor.Slot {
		return
	}
	matches[notification.Value.Signature] = e.MatchLogs(notification.Value)
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

func (e AddressEvents[S]) catchUp(ctx context.Context, session *monitord.Session[S], client *Client, cursor *addressEventsCursor, logMatches map[Signature]bool) error {
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
				return e.historyGap(ctx, session, client, cursor, logMatches, latest,
					fmt.Errorf("quicknode solana: saved signature %s is unavailable in address history", cursor.Signature))
			}
			reachedCursor = true
			break
		}
		if page+1 == maxPages {
			return e.historyGap(ctx, session, client, cursor, logMatches, latest,
				fmt.Errorf("quicknode solana: address backfill exceeded %d pages", maxPages))
		}
		before = batch[len(batch)-1].Signature
	}
	if !reachedCursor {
		return fmt.Errorf("quicknode solana: saved signature %s was not reached within %d pages", cursor.Signature, maxPages)
	}
	for i := len(pending) - 1; i >= 0; i-- {
		info := pending[i]
		matched, hinted := logMatches[info.Signature]
		delete(logMatches, info.Signature)
		var event AddressEvent
		if !hinted || matched {
			payload, err := client.GetTransaction(ctx, info.Signature, TransactionOptions{Commitment: Finalized})
			if err != nil {
				return err
			}
			if bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
				return fmt.Errorf("quicknode solana: finalized transaction %s is temporarily unavailable", info.Signature)
			}
			event = AddressEvent{
				Signature:   info.Signature,
				Slot:        info.Slot,
				Err:         append(json.RawMessage(nil), info.Err...),
				Memo:        info.Memo,
				BlockTime:   info.BlockTime,
				Transaction: append(json.RawMessage(nil), payload...),
			}
		}
		next := addressEventsCursor{
			GenesisHash: client.GenesisHash(),
			Address:     e.Address,
			Signature:   info.Signature,
			Slot:        info.Slot,
		}
		if err := session.Commit(ctx, func(tx *monitord.Tx[S]) error {
			if !hinted || matched {
				if err := e.Handle(ctx, client, tx, event); err != nil {
					return err
				}
			}
			return tx.Checkpoint(addressEventsCursorSource, next)
		}); err != nil {
			return err
		}
		*cursor = next
	}
	return nil
}

func (e AddressEvents[S]) historyGap(
	ctx context.Context,
	session *monitord.Session[S],
	client *Client,
	cursor *addressEventsCursor,
	logMatches map[Signature]bool,
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
	if err := session.Commit(ctx, func(tx *monitord.Tx[S]) error {
		return tx.Checkpoint(addressEventsCursorSource, next)
	}); err != nil {
		return err
	}
	*cursor = next
	clear(logMatches)
	return nil
}
