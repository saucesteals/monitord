package cli

import "testing"

func TestCommandSurface(t *testing.T) {
	root := New()
	for _, name := range []string{"deploy", "list", "inspect", "expire", "resume", "rm", "state", "runs", "events"} {
		if _, _, err := root.Find([]string{name}); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	if cmd, _, _ := root.Find([]string{"stats"}); cmd != root {
		t.Fatal("removed stats command remains")
	}
	if cmd, _, _ := root.Find([]string{"proxy"}); cmd != root {
		t.Fatal("removed proxy command remains")
	}
	daemon, _, _ := root.Find([]string{"daemon"})
	if daemon.Flag("concurrency") != nil {
		t.Fatal("daemon exposes unused --concurrency")
	}
	deploy, _, _ := root.Find([]string{"deploy"})
	if deploy.Flag("name") == nil {
		t.Fatal("deploy lacks --name")
	}
	resume, _, _ := root.Find([]string{"resume"})
	if resume.Flag("ttl") == nil {
		t.Fatal("resume lacks fresh --ttl")
	}
	remove, _, _ := root.Find([]string{"rm"})
	if remove.Flag("purge") == nil || remove.Flag("force") == nil {
		t.Fatal("rm lacks safe purge flags")
	}
	test, _, _ := root.Find([]string{"test"})
	if test.Flag("duration") == nil {
		t.Fatal("test lacks explicit --duration")
	}
}
