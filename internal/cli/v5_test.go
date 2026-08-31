package cli

import "testing"

func TestV5CommandSurface(t *testing.T) {
	root := New()
	for _, name := range []string{"deploy", "list", "inspect", "expire", "resume", "rm", "state", "runs", "events"} {
		if _, _, err := root.Find([]string{name}); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	if cmd, _, _ := root.Find([]string{"stats"}); cmd != root {
		t.Fatal("legacy stats command remains")
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
}
