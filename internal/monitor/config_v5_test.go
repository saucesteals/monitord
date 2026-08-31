package monitor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigDoesNotRequireDaemonSchedule(t *testing.T) {
	dir := t.TempDir()
	raw := []byte("ttl: 1h\ndeliveries:\n  - discord:\n      webhook_url: https://discord.com/api/webhooks/123456789012345678/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n")
	if err := os.WriteFile(filepath.Join(dir, ConfigFileName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Every != 0 {
		t.Fatal("operational config unexpectedly owns schedule")
	}
}
