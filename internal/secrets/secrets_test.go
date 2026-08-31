package secrets

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseDotenv(t *testing.T) {
	got, err := ParseDotenv([]byte("\ufeff# comment\r\nexport A='one two'\r\nB=three # comment\nC=\"line\\nnext\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got["A"] != "one two" || got["B"] != "three" || got["C"] != "line\nnext" {
		t.Fatalf("unexpected values: %#v", got)
	}
	if _, err := ParseDotenv([]byte("A=1\nA=2\n")); err == nil {
		t.Fatal("expected duplicate error")
	}
	if _, err := ParseDotenv([]byte("A=$(oops)\n")); err != nil {
		t.Fatal(err)
	}
}

func TestResolvePrecedenceAndExactFiltering(t *testing.T) {
	root := t.TempDir()
	monitor := t.TempDir()
	mustWrite(t, filepath.Join(root, ".env"), "TOKEN=global\nUNREQUESTED=leak\n")
	if err := os.Mkdir(filepath.Join(root, "secrets"), 0o700); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(root, "secrets", "quicknode.env"), "TOKEN=group\n")
	mustWrite(t, filepath.Join(monitor, ".env"), "TOKEN=local\n")
	values, err := Resolve([]Ref{{Group: "quicknode", Key: "TOKEN", Required: true}}, Sources{Root: root, MonitorDir: monitor, Overrides: map[string]string{"quicknode/TOKEN": "override"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Value != "override" || values[0].Source != "override" {
		t.Fatalf("unexpected: %#v", values)
	}
	if got := Redact("url=override safe", values); got != "url=[REDACTED] safe" {
		t.Fatalf("redact: %q", got)
	}
}

func TestSecureFilesAndFingerprint(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, ".env")
	mustWrite(t, path, "A=x\n")
	values, err := Resolve([]Ref{{Group: "g", Key: "A", Required: true}}, Sources{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	a := Fingerprint([]byte("key"), values)
	b := Fingerprint([]byte("key"), values)
	if a == "" || a != b {
		t.Fatal("fingerprint is not stable")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve([]Ref{{Group: "g", Key: "A"}}, Sources{Root: root}); err == nil {
		t.Fatal("expected insecure mode error")
	}
}

func mustWrite(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}
