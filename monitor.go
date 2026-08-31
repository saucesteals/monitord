// Package monitord is the authoring SDK for durable monitord V5 monitors.
package monitord

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"
)

type Info struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

var infoNamePattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

func (i Info) Validate() error {
	if i.Name == "" {
		return errors.New("monitor name is required")
	}
	if len(i.Name) > 63 {
		return fmt.Errorf("monitor name must be at most 63 bytes, got %d", len(i.Name))
	}
	if !infoNamePattern.MatchString(i.Name) {
		return fmt.Errorf("monitor name %q must be lower-case kebab case", i.Name)
	}
	return nil
}

type Monitor[S any] interface {
	Info() Info
	Plan() Plan[S]
}
type definedMonitor[S any] struct {
	info Info
	plan Plan[S]
}

func (m definedMonitor[S]) Info() Info                 { return m.info }
func (m definedMonitor[S]) Plan() Plan[S]              { return m.plan }
func Define[S any](info Info, plan Plan[S]) Monitor[S] { return definedMonitor[S]{info, plan} }

// Run implements the V5 monitor executable contract and does not return.
func Run[S any](monitor Monitor[S]) {
	if err := dispatchMonitor(monitor); err != nil {
		panic(err)
	}
}

type planKind uint8

const (
	planInvalid planKind = iota
	planContinuous
	planEvery
	planNamed
	planCombined
)

type Plan[S any] struct{ node *planNode[S] }
type planNode[S any] struct {
	kind       planKind
	continuous ContinuousFunc[S]
	check      CheckFunc[S]
	interval   time.Duration
	name       string
	optional   bool
	options    commonOptions
	child      *planNode[S]
	children   []*planNode[S]
}
type ContinuousFunc[S any] func(context.Context, *Session[S]) error
type CheckFunc[S any] func(context.Context, *Session[S]) error
type commonOptions struct {
	secrets []SecretRef
	timeout time.Duration
	err     error
}
type ContinuousOption interface{ applyContinuous(*commonOptions) error }
type ScheduleOption interface{ applySchedule(*commonOptions) error }
type CommonOption interface {
	ContinuousOption
	ScheduleOption
}
type ChildOption interface{ applyChild(*childOptions) error }
type childOptions struct{ optional bool }
type optionFunc func(*commonOptions) error

func (f optionFunc) applyContinuous(o *commonOptions) error { return f(o) }
func (f optionFunc) applySchedule(o *commonOptions) error   { return f(o) }

type childOptionFunc func(*childOptions) error

func (f childOptionFunc) applyChild(o *childOptions) error { return f(o) }
func WithTimeout(v time.Duration) CommonOption {
	return optionFunc(func(o *commonOptions) error {
		if v < 0 {
			return errors.New("callback timeout cannot be negative")
		}
		o.timeout = v
		return nil
	})
}
func Optional() ChildOption {
	return childOptionFunc(func(o *childOptions) error { o.optional = true; return nil })
}
func Continuous[S any](fn ContinuousFunc[S], opts ...ContinuousOption) Plan[S] {
	n := &planNode[S]{kind: planContinuous, continuous: fn}
	for _, o := range opts {
		if o == nil {
			n.options.err = errors.Join(n.options.err, errors.New("nil continuous option"))
		} else {
			n.options.err = errors.Join(n.options.err, o.applyContinuous(&n.options))
		}
	}
	return Plan[S]{n}
}
func Every[S any](interval time.Duration, fn CheckFunc[S], opts ...ScheduleOption) Plan[S] {
	n := &planNode[S]{kind: planEvery, interval: interval, check: fn}
	for _, o := range opts {
		if o == nil {
			n.options.err = errors.Join(n.options.err, errors.New("nil schedule option"))
		} else {
			n.options.err = errors.Join(n.options.err, o.applySchedule(&n.options))
		}
	}
	return Plan[S]{n}
}
func Named[S any](name string, p Plan[S], opts ...ChildOption) Plan[S] {
	c := childOptions{}
	var e error
	for _, o := range opts {
		if o == nil {
			e = errors.Join(e, errors.New("nil child option"))
		} else {
			e = errors.Join(e, o.applyChild(&c))
		}
	}
	return Plan[S]{&planNode[S]{kind: planNamed, name: name, optional: c.optional, child: p.node, options: commonOptions{err: e}}}
}
func Combined[S any](plans ...Plan[S]) Plan[S] {
	n := &planNode[S]{kind: planCombined, children: make([]*planNode[S], len(plans))}
	for i := range plans {
		n.children[i] = plans[i].node
	}
	return Plan[S]{n}
}

type PlanDescription struct {
	Kind     string            `json:"kind"`
	Name     string            `json:"name,omitempty"`
	Optional bool              `json:"optional,omitempty"`
	Interval time.Duration     `json:"interval,omitempty"`
	Timeout  time.Duration     `json:"timeout,omitempty"`
	Secrets  []SecretRef       `json:"secrets,omitempty"`
	Children []PlanDescription `json:"children,omitempty"`
}

func validateMonitor[S any](m Monitor[S]) (PlanDescription, error) {
	if m == nil {
		return PlanDescription{}, errors.New("monitor is nil")
	}
	if err := m.Info().Validate(); err != nil {
		return PlanDescription{}, err
	}
	return normalizePlan(m.Plan(), false)
}
func normalizePlan[S any](p Plan[S], nested bool) (PlanDescription, error) {
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
	case planNamed:
		if len(n.name) > 63 || !infoNamePattern.MatchString(n.name) {
			return d, fmt.Errorf("invalid child name %q", n.name)
		}
		c, e := normalizePlan(Plan[S]{n.child}, true)
		if e != nil {
			return d, fmt.Errorf("child %q: %w", n.name, e)
		}
		if c.Kind == "named" || c.Kind == "combined" {
			return d, fmt.Errorf("child %q wraps unsupported %s plan", n.name, c.Kind)
		}
		d = PlanDescription{Kind: "named", Name: n.name, Optional: n.optional, Children: []PlanDescription{c}}
	case planCombined:
		if nested {
			return d, errors.New("nested combined plans are unsupported")
		}
		if len(n.children) == 0 {
			return d, errors.New("combined plan is empty")
		}
		d.Kind = "combined"
		seen := map[string]struct{}{}
		for _, cn := range n.children {
			c, e := normalizePlan(Plan[S]{cn}, true)
			if e != nil {
				return d, e
			}
			if c.Kind != "named" {
				return d, errors.New("every combined child must be named")
			}
			if _, ok := seen[c.Name]; ok {
				return d, fmt.Errorf("duplicate child name %q", c.Name)
			}
			seen[c.Name] = struct{}{}
			d.Children = append(d.Children, c)
		}
	default:
		return d, errors.New("unsupported plan shape")
	}
	return d, nil
}
func (d PlanDescription) SecretRefs() []SecretRef {
	r := append([]SecretRef(nil), d.Secrets...)
	for _, c := range d.Children {
		r = append(r, c.SecretRefs()...)
	}
	r, _ = normalizeSecretRefs(r)
	sort.Slice(r, func(i, j int) bool {
		if r[i].Group == r[j].Group {
			return r[i].Key < r[j].Key
		}
		return r[i].Group < r[j].Group
	})
	return r
}
