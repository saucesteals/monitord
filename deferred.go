package monitord

// Deferred reports that no source attempt was made. Polling continues at the
// normal interval; the monitor owns any durable retry deadline in its state.
// It neither counts as a failure nor proves recovery. Upgrade the daemon before
// deploying workers that return this outcome.
func Deferred(reason string) error {
	if reason == "" {
		reason = "source attempt deferred"
	}
	return &deferredError{reason: reason}
}

type deferredError struct{ reason string }

func (e *deferredError) Error() string { return e.reason }
