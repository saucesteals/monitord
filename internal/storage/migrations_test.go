package storage

import "testing"

func TestSchemaHasSingleRuntimeIdentity(t *testing.T) {
	s := testStore(t)
	for _, table := range []string{"monitors", "runs", "events"} {
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("obsolete runtime table %q still exists", table)
		}
	}
	for _, table := range []string{"routes", "deployments", "transactions", "outbox_deliveries"} {
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("required table %q is missing", table)
		}
	}
}

func TestSchemaIntegrityAndDurabilityPragmas(t *testing.T) {
	s := testStore(t)
	checks := []struct {
		pragma string
		want   string
	}{
		{"journal_mode", "wal"},
		{"synchronous", "2"},
		{"foreign_keys", "1"},
		{"busy_timeout", "5000"},
	}
	for _, check := range checks {
		var got string
		if err := s.db.QueryRow(`PRAGMA ` + check.pragma).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", check.pragma, err)
		}
		if got != check.want {
			t.Errorf("%s = %q, want %q", check.pragma, got, check.want)
		}
	}
	rows, err := s.db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("fresh schema has a foreign-key violation")
	}
}
