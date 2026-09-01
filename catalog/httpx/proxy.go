package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	http "github.com/saucesteals/fhttp"
	"github.com/saucesteals/mimic"
	"github.com/saucesteals/monitord"
)

const chromiumVersion = "131.0.0.0"

// NewClient creates a reusable browser-compatible client using the host's
// direct network path.
func NewClient() (*http.Client, error) {
	return newClient(nil)
}

// ProxyClient distributes requests across a fixed set of proxy-bound clients.
// Each proxy has its own transport so connections and TLS sessions remain
// attached to the same exit between checks.
type ProxyClient struct {
	clients []*http.Client
	next    atomic.Uint64
}

// NewProxyClient loads a JSON array of proxy URLs from ref and creates one
// reusable client per proxy.
func NewProxyClient(secrets monitord.SecretSet, ref monitord.SecretRef) (*ProxyClient, error) {
	if secrets == nil {
		return nil, errors.New("proxy secrets are unavailable")
	}
	raw, err := secrets.Require(ref)
	if err != nil {
		return nil, fmt.Errorf("proxy secret %s/%s is unavailable", ref.Group, ref.Key)
	}

	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, errors.New("proxy secret must be a JSON array of URL strings")
	}
	if len(values) == 0 {
		return nil, errors.New("proxy secret contains no URLs")
	}

	clients := make([]*http.Client, len(values))
	for i, value := range values {
		proxy, ok := parseProxy(value)
		if !ok {
			return nil, fmt.Errorf("proxy entry %d is not a supported URL", i)
		}
		client, err := newClient(proxy)
		if err != nil {
			return nil, fmt.Errorf("create client for proxy entry %d", i)
		}
		clients[i] = client
	}

	return &ProxyClient{clients: clients}, nil
}

// Do sends req through the next proxy in round-robin order. Redirects remain
// on the selected client and therefore on the same proxy.
func (c *ProxyClient) Do(req *http.Request) (*http.Response, error) {
	if c == nil || len(c.clients) == 0 {
		return nil, errors.New("proxy client is not initialized")
	}
	index := c.next.Add(1) - 1
	return c.clients[index%uint64(len(c.clients))].Do(req)
}

// CloseIdleConnections closes idle connections held by every proxy transport.
func (c *ProxyClient) CloseIdleConnections() {
	if c == nil {
		return
	}
	for _, client := range c.clients {
		client.CloseIdleConnections()
	}
}

func parseProxy(raw string) (*url.URL, bool) {
	proxy, err := url.Parse(raw)
	if err != nil || proxy.Opaque != "" || proxy.Hostname() == "" || proxy.Fragment != "" || proxy.RawQuery != "" || proxy.ForceQuery {
		return nil, false
	}
	if proxy.Path != "" && proxy.Path != "/" {
		return nil, false
	}
	switch proxy.Scheme {
	case "http", "https", "socks5":
		return proxy, true
	default:
		return nil, false
	}
}

func newClient(proxy *url.URL) (*http.Client, error) {
	var proxyFunc func(*http.Request) (*url.URL, error)
	if proxy != nil {
		proxyFunc = http.ProxyURL(proxy)
	}
	base := &http.Transport{
		Proxy: proxyFunc,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	transport, err := mimic.NewTransport(mimic.TransportOptions{
		Version:   chromiumVersion,
		Brand:     mimic.BrandChrome,
		Platform:  mimic.PlatformMac,
		Transport: base,
	})
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}
