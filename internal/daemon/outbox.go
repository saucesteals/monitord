package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
)

const (
	outboxLease       = 30 * time.Second
	outboxBatch       = 32
	outboxBaseBackoff = 2 * time.Second
	outboxMaxBackoff  = 15 * time.Minute
)

// DeliverySender performs the only non-transactional step in the durable
// outbox. Implementations must be safe to retry: a successful send followed by
// marker loss is explicitly allowed to duplicate externally.
type DeliverySender interface {
	Send(context.Context, storage.ClaimedDelivery) error
}

type daemonDeliverySender struct{ daemon *Daemon }

func (s daemonDeliverySender) Send(ctx context.Context, delivery storage.ClaimedDelivery) error {
	var binding routes.Delivery
	if err := json.Unmarshal(delivery.DestinationConfig, &binding); err != nil {
		return permanentDeliveryError{fmt.Errorf("decode destination binding: %w", err)}
	}
	var event monitord.Event
	if err := json.Unmarshal(delivery.EventPayload, &event); err != nil {
		return permanentDeliveryError{fmt.Errorf("decode event: %w", err)}
	}
	return s.daemon.deliverRoute(ctx, binding, eventMessage(delivery.DeploymentName, delivery.CreatedAt, event))
}

type permanentDeliveryError struct{ error }

func (permanentDeliveryError) Permanent() bool           { return true }
func (permanentDeliveryError) RetryAfter() time.Duration { return 0 }

func eventMessage(deployment string, createdAt time.Time, event monitord.Event) routes.Message {
	return routes.Message{
		Title: event.Title, Message: event.Body, URL: event.URL,
		Level: eventLevel(event.Severity), Fields: dataFields(event.Data),
		Footer: deployment, Time: createdAt,
	}
}

type deliveryError interface {
	error
	Permanent() bool
	RetryAfter() time.Duration
}

type outboxStore interface {
	ClaimOutbox(context.Context, string, time.Time, time.Duration, int) ([]storage.ClaimedDelivery, error)
	MarkDelivered(context.Context, string, string, string, time.Time) error
	MarkDeliveryFailed(context.Context, string, string, string, string, time.Time, time.Time, int) error
}

type outboxWorker struct {
	store  outboxStore
	sender DeliverySender
	owner  string
	now    func() time.Time
}

func newOutboxWorker(store outboxStore, sender DeliverySender, owner string) *outboxWorker {
	return &outboxWorker{store: store, sender: sender, owner: owner, now: func() time.Time { return time.Now().UTC() }}
}

func (w *outboxWorker) process(ctx context.Context) (int, error) {
	now := w.now()
	claimed, err := w.store.ClaimOutbox(ctx, w.owner, now, outboxLease, outboxBatch)
	if err != nil {
		return 0, err
	}
	results := make(chan error, len(claimed))
	for _, delivery := range claimed {
		go func() { results <- w.sendOne(ctx, delivery) }()
	}
	var joined error
	for range claimed {
		if err := <-results; err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return len(claimed), joined
}

func (w *outboxWorker) sendOne(ctx context.Context, delivery storage.ClaimedDelivery) error {
	attemptCtx, cancel := context.WithTimeout(ctx, deliveryAttemptTimeout)
	result := make(chan error, 1)
	go func() { result <- w.sender.Send(attemptCtx, delivery) }()
	var err error
	select {
	case err = <-result:
	case <-attemptCtx.Done():
		err = attemptCtx.Err()
	}
	cancel()
	now := w.now()
	if err == nil {
		return w.store.MarkDelivered(ctx, delivery.OutboxID, delivery.DestinationID, delivery.LeaseOwner, now)
	}
	maxAttempts := math.MaxInt
	delay := retryDelay(delivery.AttemptCount)
	var classified deliveryError
	if errors.As(err, &classified) {
		if classified.Permanent() {
			maxAttempts = 1
		}
		if after := classified.RetryAfter(); after > 0 {
			delay = min(after, outboxMaxBackoff)
		}
	}
	if markErr := w.store.MarkDeliveryFailed(ctx, delivery.OutboxID, delivery.DestinationID, delivery.LeaseOwner, err.Error(), now, now.Add(delay), maxAttempts); markErr != nil {
		return errors.Join(fmt.Errorf("send %s/%s: %w", delivery.OutboxID, delivery.DestinationID, err), markErr)
	}
	return nil
}

func retryDelay(attempt int) time.Duration {
	// Full jitter avoids synchronizing many destinations after a provider
	// outage. attempt is the number already completed before this lease.
	shift := min(max(attempt, 0), 20)
	capDelay := min(outboxBaseBackoff*time.Duration(1<<shift), outboxMaxBackoff)
	if capDelay <= time.Millisecond {
		return capDelay
	}
	return time.Duration(rand.Int64N(int64(capDelay)))
}
