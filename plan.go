package monitord

import (
	"context"
	"errors"
	"sort"
	"time"
)

type planKind uint8

const (
	planInvalid planKind = iota
	planContinuous
	planEvery
)

type Plan[S any] struct{ node *planNode[S] }
type planNode[S any] struct {
	kind       planKind
	continuous ContinuousFunc[S]
	check      CheckFunc[S]
	interval   time.Duration
	options    commonOptions
}
type ContinuousFunc[S any] func(context.Context, *Session[S]) error
type CheckFunc[S any] func(context.Context, *Session[S]) error
type commonOptions struct {
	secrets []SecretRef
	timeout time.Duration
	err     error
}
type CommonOption interface{ apply(*commonOptions) error }
type optionFunc func(*commonOptions) error

func (f optionFunc) apply(o *commonOptions) error { return f(o) }

func WithTimeout(v time.Duration) CommonOption {
	return optionFunc(func(o *commonOptions) error {
		if v < 0 {
			return errors.New("callback timeout cannot be negative")
		}
		o.timeout = v
		return nil
	})
}

func Continuous[S any](fn ContinuousFunc[S], opts ...CommonOption) Plan[S] {
	return Plan[S]{&planNode[S]{kind: planContinuous, continuous: fn, options: applyOptions(opts)}}
}

func Every[S any](interval time.Duration, fn CheckFunc[S], opts ...CommonOption) Plan[S] {
	return Plan[S]{&planNode[S]{kind: planEvery, interval: interval, check: fn, options: applyOptions(opts)}}
}

func applyOptions(opts []CommonOption) commonOptions {
	var result commonOptions
	for _, option := range opts {
		if option == nil {
			result.err = errors.Join(result.err, errors.New("nil plan option"))
			continue
		}
		result.err = errors.Join(result.err, option.apply(&result))
	}
	return result
}

type PlanDescription struct {
	Kind     string        `json:"kind"`
	Interval time.Duration `json:"interval,omitempty"`
	Timeout  time.Duration `json:"timeout,omitempty"`
	Secrets  []SecretRef   `json:"secrets,omitempty"`
}

func validateMonitor[S any](m Monitor[S]) (PlanDescription, error) {
	if m == nil {
		return PlanDescription{}, errors.New("monitor is nil")
	}
	if err := m.Info().Validate(); err != nil {
		return PlanDescription{}, err
	}
	return normalizePlan(m.Plan())
}

func normalizePlan[S any](p Plan[S]) (PlanDescription, error) {
	if p.node == nil {
		return PlanDescription{}, errors.New("plan is zero or invalid")
	}
	n := p.node
	if n.options.err != nil {
		return PlanDescription{}, n.options.err
	}
	refs, err := normalizeSecretRefs(n.options.secrets)
	if err != nil {
		return PlanDescription{}, err
	}
	d := PlanDescription{Timeout: n.options.timeout, Secrets: refs}
	switch n.kind {
	case planContinuous:
		if n.continuous == nil {
			return d, errors.New("continuous callback is nil")
		}
		d.Kind = "continuous"
	case planEvery:
		if n.check == nil {
			return d, errors.New("poll callback is nil")
		}
		if n.interval <= 0 {
			return d, errors.New("poll interval must be positive")
		}
		d.Kind = "every"
		d.Interval = n.interval
	default:
		return d, errors.New("unsupported plan")
	}
	return d, nil
}

func (d PlanDescription) SecretRefs() []SecretRef {
	refs, _ := normalizeSecretRefs(d.Secrets)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Group == refs[j].Group {
			return refs[i].Key < refs[j].Key
		}
		return refs[i].Group < refs[j].Group
	})
	return refs
}
