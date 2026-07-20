package network

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
)

// DefaultScheme is assumed when a proxy line does not name one.
const DefaultScheme = "http"

// ParseProxy normalises one proxy line into a URL.
//
// Every format these lists ship in is accepted, because which one you get
// depends on the vendor and nobody should have to rewrite a file to import it:
//
//	host:port:user:pass          the common provider export
//	host:port
//	user:pass@host:port
//	scheme://user:pass@host:port
//	scheme://host:port
func ParseProxy(line string) (string, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", fmt.Errorf("empty proxy")
	}

	if strings.Contains(line, "://") {
		return normalizeURL(line)
	}

	// user:pass@host:port, but only when the text after the last "@" really is
	// a host and port. A password containing "@" would otherwise be mistaken
	// for the separator, so the colon-delimited form below handles that case.
	if at := strings.LastIndex(line, "@"); at > 0 {
		if host, port, ok := strings.Cut(line[at+1:], ":"); ok && host != "" && isNumeric(port) {
			user, pass, _ := strings.Cut(line[:at], ":")

			return normalizeURL(fmt.Sprintf("%s://%s:%s@%s:%s",
				DefaultScheme, url.QueryEscape(user), url.QueryEscape(pass), host, port))
		}
	}

	// Split at most four ways so a password containing ":" survives intact —
	// provider passwords routinely do.
	switch parts := strings.SplitN(line, ":", 4); len(parts) {
	case 2:
		return normalizeURL(fmt.Sprintf("%s://%s:%s", DefaultScheme, parts[0], parts[1]))
	case 4:
		// host:port:user:pass — credentials are escaped because provider
		// passwords routinely contain characters that are not URL-safe.
		return normalizeURL(fmt.Sprintf("%s://%s:%s@%s:%s",
			DefaultScheme,
			url.QueryEscape(parts[2]), url.QueryEscape(parts[3]),
			parts[0], parts[1],
		))
	default:
		return "", fmt.Errorf("unrecognised proxy %q: want host:port, host:port:user:pass, or a URL", Redact(line))
	}
}

func isNumeric(value string) bool {
	if value == "" {
		return false
	}
	_, err := strconv.Atoi(value)

	return err == nil
}

func normalizeURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse proxy %q: %w", Redact(raw), err)
	}
	if parsed.Hostname() == "" {
		return "", fmt.Errorf("proxy %q has no host", Redact(raw))
	}

	port := parsed.Port()
	if port == "" {
		return "", fmt.Errorf("proxy %q has no port", Redact(raw))
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", fmt.Errorf("proxy %q has a non-numeric port", Redact(raw))
	}

	return parsed.String(), nil
}

// ParseProxies reads a proxy list, one per line. Blank lines and # comments are
// skipped. It reports every bad line rather than only the first, so a large
// import can be fixed in one pass.
func ParseProxies(r io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		proxies  []string
		problems []string
		seen     = map[string]bool{}
		line     int
	)

	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}

		proxy, err := ParseProxy(text)
		if err != nil {
			if len(problems) < 10 {
				problems = append(problems, fmt.Sprintf("line %d: %v", line, err))
			}

			continue
		}
		if seen[proxy] {
			continue
		}
		seen[proxy] = true
		proxies = append(proxies, proxy)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read proxies: %w", err)
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("invalid proxies:\n  %s", strings.Join(problems, "\n  "))
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("no proxies found")
	}

	return proxies, nil
}

// Redact strips credentials from a proxy so it can appear in errors and logs.
func Redact(proxy string) string {
	parsed, err := url.Parse(proxy)
	if err == nil && parsed.Host != "" {
		parsed.User = nil

		return parsed.String()
	}

	// Fall back to field position for lines that never parsed as a URL.
	if parts := strings.Split(proxy, ":"); len(parts) >= 2 {
		return parts[0] + ":" + parts[1]
	}

	return "<proxy>"
}
