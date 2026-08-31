package monitord

import (
	"context"
	"testing"
	"time"
)

func TestPlanValidationAndSecrets(t *testing.T) {
	fn := func(context.Context, *Session[struct{}]) error { return nil }
	p := Combined(Named("required", Continuous(fn, WithSecrets(OptionalSecret("api", "TOKEN"), RequiredSecret("api", "TOKEN")))), Named("optional", Every(time.Minute, fn), Optional()))
	d, err := validateMonitor(Define(Info{Name: "valid-monitor"}, p))
	if err != nil {
		t.Fatal(err)
	}
	refs := d.SecretRefs()
	if len(refs) != 1 || !refs[0].Required {
		t.Fatalf("refs=%+v", refs)
	}
	cases := []Monitor[struct{}]{Define(Info{Name: "Bad"}, Continuous(fn)), Define(Info{Name: "ok"}, Plan[struct{}]{}), Define(Info{Name: "ok"}, Combined[struct{}]()), Define(Info{Name: "ok"}, Combined(Continuous(fn))), Define(Info{Name: "ok"}, Combined(Named("x", Continuous(fn)), Named("x", Continuous(fn))))}
	for i, m := range cases {
		if _, err := validateMonitor(m); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}

type compileMonitor struct{}

func (compileMonitor) Info() Info { return Info{Name: "compile-monitor"} }
func (compileMonitor) Plan() Plan[struct{}] {
	return Continuous(func(context.Context, *Session[struct{}]) error { return nil })
}

var _ Monitor[struct{}] = compileMonitor{}
var _ = func() {
	if false {
		Run(compileMonitor{})
		Run[struct{}](compileMonitor{})
	}
}
