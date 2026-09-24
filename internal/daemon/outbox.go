package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/saucesteals/monitord/internal/delivery"
	"github.com/saucesteals/monitord/internal/storage"
	"golang.org/x/time/rate"
)

const (
	outboxLease       = deliveryAttemptTimeout + 5*time.Second
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

func (s daemonDeliverySender) Send(ctx context.Context, claimed storage.ClaimedDelivery) error {
	var binding delivery.Delivery
	if err := json.Unmarshal(claimed.DestinationConfig, &binding); err != nil {
		return permanentDeliveryError{fmt.Errorf("decode destination binding: %w", err)}
	}
	if err := binding.Validate(); err != nil {
		return permanentDeliveryError{fmt.Errorf("invalid destination binding: %w", err)}
	}
	var message delivery.Message
	if err := json.Unmarshal(claimed.MessagePayload, &message); err != nil {
		return permanentDeliveryError{fmt.Errorf("decode message: %w", err)}
	}
	return s.daemon.deliverDestination(ctx, binding, message)
}

type permanentDeliveryError struct{ error }

func (permanentDeliveryError) Permanent() bool           { return true }
func (permanentDeliveryError) RetryAfter() time.Duration { return 0 }

type deliveryError interface {
	error
	Permanent() bool
	RetryAfter() time.Duration
}

type outboxStore interface {
	ClaimOutbox(context.Context, string, time.Time, time.Duration, int) ([]storage.ClaimedDelivery, error)
	DeferDelivery(context.Context, string, string, string, time.Time) error
	MarkDelivered(context.Context, string, string, string, time.Time) error
	MarkDeliveryFailed(context.Context, string, string, string, string, time.Time, time.Time, int) error
}

type outboxWorker struct {
	store    outboxStore
	sender   DeliverySender
	owner    string
	now      func() time.Time
	limitsMu sync.Mutex
	limits   map[destinationLimitKey]*rate.Limiter
}

func newOutboxWorker(store outboxStore, sender DeliverySender, owner string) *outboxWorker {
	return &outboxWorker{
		store: store, sender: sender, owner: owner,
		now:    func() time.Time { return time.Now().UTC() },
		limits: make(map[destinationLimitKey]*rate.Limiter),
	}
}

type destinationLimitKey struct {
	deploymentID, destinationID string
	revision                    int64
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
	var err error
	throttleDelay, err := w.reserveDestination(delivery)
	if err == nil && throttleDelay > 0 {
		settleCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return w.store.DeferDelivery(settleCtx, delivery.OutboxID, delivery.DestinationID, delivery.LeaseOwner, w.now().Add(throttleDelay))
	}
	if err == nil {
		attemptCtx, cancel := context.WithTimeout(ctx, deliveryAttemptTimeout)
		result := make(chan error, 1)
		go func() { result <- w.sender.Send(attemptCtx, delivery) }()
		select {
		case err = <-result:
		case <-attemptCtx.Done():
			err = attemptCtx.Err()
		}
		cancel()
	}
	now := w.now()
	settleCtx, settleCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer settleCancel()
	if err == nil {
		return w.store.MarkDelivered(settleCtx, delivery.OutboxID, delivery.DestinationID, delivery.LeaseOwner, now)
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
	if markErr := w.store.MarkDeliveryFailed(settleCtx, delivery.OutboxID, delivery.DestinationID, delivery.LeaseOwner, err.Error(), now, now.Add(delay), maxAttempts); markErr != nil {
		return fmt.Errorf("record failed delivery %s/%s: %w", delivery.OutboxID, delivery.DestinationID, markErr)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return fmt.Errorf("delivery %s/%s failed", delivery.OutboxID, delivery.DestinationID)
}

func (w *outboxWorker) reserveDestination(claimed storage.ClaimedDelivery) (time.Duration, error) {
	var binding delivery.Delivery
	if err := json.Unmarshal(claimed.DestinationConfig, &binding); err != nil {
		return 0, permanentDeliveryError{fmt.Errorf("decode destination binding: %w", err)}
	}
	if err := binding.RateLimit.Validate(); err != nil {
		return 0, permanentDeliveryError{fmt.Errorf("invalid destination rate limit: %w", err)}
	}
	if !binding.RateLimit.Enabled() {
		return 0, nil
	}

	key := destinationLimitKey{claimed.DeploymentID, claimed.DestinationID, claimed.DestinationRevision}
	w.limitsMu.Lock()
	limiter := w.limits[key]
	if limiter == nil {
		limiter = rate.NewLimiter(rate.Limit(binding.RateLimit.PerSecond), binding.RateLimit.Burst)
		w.limits[key] = limiter
	}
	w.limitsMu.Unlock()
	now := w.now()
	reservation := limiter.ReserveN(now, 1)
	if !reservation.OK() {
		return 0, permanentDeliveryError{errors.New("destination rate limit cannot reserve one delivery")}
	}
	delay := reservation.DelayFrom(now)
	if delay <= 0 {
		return 0, nil
	}
	reservation.CancelAt(now)
	return delay, nil
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
