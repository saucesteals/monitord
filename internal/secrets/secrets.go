// Package secrets resolves the exact secret keys declared by a V5 monitor.
package secrets

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
)

type Ref struct {
	Group    string
	Key      string
	Required bool
}

type Value struct {
	Ref    Ref
	Value  string
	Source string
}

type Sources struct {
	Root       string
	MonitorDir string
	Overrides  map[string]string
	Defaults   map[string]string
}

func RefKey(group, key string) string { return group + "/" + key }

func ValidateRef(ref Ref) error {
	if !safeComponent(ref.Group) {
		return fmt.Errorf("invalid secret group %q", ref.Group)
	}
	if ref.Key == "" || strings.ContainsAny(ref.Key, "=\x00\r\n") {
		return fmt.Errorf("invalid secret key %q", ref.Key)
	}
	for _, r := range ref.Key {
		if !(r == '_' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return fmt.Errorf("invalid secret key %q", ref.Key)
		}
	}
	return nil
}

func safeComponent(s string) bool {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, `/\\\x00`) {
		return false
	}
	for _, r := range s {
		if !(r == '-' || r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func Normalize(refs []Ref) ([]Ref, error) {
	merged := make(map[string]Ref, len(refs))
	for _, ref := range refs {
		if err := ValidateRef(ref); err != nil {
			return nil, err
		}
		key := RefKey(ref.Group, ref.Key)
		if old, ok := merged[key]; ok {
			ref.Required = ref.Required || old.Required
		}
		merged[key] = ref
	}
	out := make([]Ref, 0, len(merged))
	for _, ref := range merged {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return RefKey(out[i].Group, out[i].Key) < RefKey(out[j].Group, out[j].Key) })
	return out, nil
}

// ParseDotenv implements a deliberately small dotenv grammar. It never expands
// variables or executes substitutions.
func ParseDotenv(data []byte) (map[string]string, error) {
	data = []byte(strings.TrimPrefix(string(data), "\ufeff"))
	if !utf8.Valid(data) {
		return nil, errors.New("dotenv is not valid UTF-8")
	}
	values := map[string]string{}
	s := bufio.NewScanner(strings.NewReader(string(data)))
	line := 0
	for s.Scan() {
		line++
		raw := strings.TrimSpace(strings.TrimSuffix(s.Text(), "\r"))
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		if strings.HasPrefix(raw, "export ") {
			raw = strings.TrimSpace(strings.TrimPrefix(raw, "export "))
		}
		key, value, ok := strings.Cut(raw, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("dotenv line %d is invalid", line)
		}
		if err := ValidateRef(Ref{Group: "dotenv", Key: key}); err != nil {
			return nil, fmt.Errorf("dotenv line %d: %w", line, err)
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("dotenv line %d duplicates %s", line, key)
		}
		value = strings.TrimSpace(value)
		parsed, err := parseValue(value)
		if err != nil {
			return nil, fmt.Errorf("dotenv line %d: %w", line, err)
		}
		values[key] = parsed
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func parseValue(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if value[0] != '\'' && value[0] != '"' {
		if i := strings.Index(value, " #"); i >= 0 {
			value = strings.TrimSpace(value[:i])
		}
		return value, nil
	}
	quote := value[0]
	if len(value) < 2 || value[len(value)-1] != quote {
		return "", errors.New("unterminated quoted value")
	}
	body := value[1 : len(value)-1]
	if quote == '\'' {
		return body, nil
	}
	r := strings.NewReplacer(`\\`, `\`, `\"`, `"`, `\n`, "\n", `\r`, "\r", `\t`, "\t")
	return r.Replace(body), nil
}

func readSecure(path string) (map[string]string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("secret source %s is not a regular file", path)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != os.Getuid() {
		return nil, fmt.Errorf("secret source %s is not owned by the daemon user", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("secret source %s must not be group/world accessible", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	after, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, after) || info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
		return nil, fmt.Errorf("secret source %s changed while it was read", path)
	}
	return ParseDotenv(data)
}

// Resolve reads each source once and returns only requested values. Empty values
// are treated as unset. Overrides are keyed by group/key; file sources by key.
func Resolve(refs []Ref, src Sources) ([]Value, error) {
	refs, err := Normalize(refs)
	if err != nil {
		return nil, err
	}
	global, err := readSecure(filepath.Join(src.Root, ".env"))
	if err != nil {
		return nil, err
	}
	local := map[string]string{}
	if src.MonitorDir != "" {
		local, err = readSecure(filepath.Join(src.MonitorDir, ".env"))
		if err != nil {
			return nil, err
		}
	}
	groups := map[string]map[string]string{}
	out := make([]Value, 0, len(refs))
	for _, ref := range refs {
		key := RefKey(ref.Group, ref.Key)
		value, source := src.Overrides[key], "override"
		if value == "" {
			value, source = local[ref.Key], "monitor"
		}
		if value == "" {
			group, ok := groups[ref.Group]
			if !ok {
				group, err = readSecure(filepath.Join(src.Root, "secrets", ref.Group+".env"))
				if err != nil {
					return nil, err
				}
				groups[ref.Group] = group
			}
			value, source = group[ref.Key], "group"
		}
		if value == "" {
			value, source = global[ref.Key], "global"
		}
		if value == "" {
			value, source = src.Defaults[key], "default"
		}
		if value == "" {
			if ref.Required {
				return nil, fmt.Errorf("required secret %s is unresolved", key)
			}
			continue
		}
		out = append(out, Value{Ref: ref, Value: value, Source: source})
	}
	return out, nil
}

func Fingerprint(key []byte, values []Value) string {
	mac := hmac.New(sha256.New, key)
	ordered := append([]Value(nil), values...)
	sort.Slice(ordered, func(i, j int) bool {
		return RefKey(ordered[i].Ref.Group, ordered[i].Ref.Key) < RefKey(ordered[j].Ref.Group, ordered[j].Ref.Key)
	})
	for _, value := range ordered {
		for _, part := range []string{value.Ref.Group, value.Ref.Key, value.Value} {
			fmt.Fprintf(mac, "%d:", len(part))
			_, _ = mac.Write([]byte(part))
		}
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func Redact(message string, values []Value) string {
	parts := make([]string, 0, len(values)*2)
	for _, value := range values {
		if value.Value != "" {
			parts = append(parts, value.Value, "[REDACTED]")
		}
	}
	return strings.NewReplacer(parts...).Replace(message)
}
