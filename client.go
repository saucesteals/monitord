package monitord

import (
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	fhttp "github.com/saucesteals/fhttp"
	"github.com/saucesteals/mimic"
)

// DefaultMimicVersion is the impersonated Chrome version.
const DefaultMimicVersion = "150.0.0.0"

// Client is the HTTP client type handed to monitors: saucesteals/fhttp driven
// by a saucesteals/mimic transport.
//
// Every client is mimic-backed. fhttp's own TLS stack cannot verify common
// certificate chains, and its DialTLSContext hook is bypassed entirely once a
// proxy is in play, so a "plain" fhttp transport fails on most HTTPS targets
// and on all proxied ones. mimic supplies the TLS layer that actually works,
// and browser-like TLS is what a monitor hitting third-party services wants
// anyway.
type Client = fhttp.Client

// Clients is a fixed cycle of HTTP clients, one per proxy assigned to this
// worker by the daemon.
//
// Each client owns a dedicated transport and therefore its own connection pool,
// keepalive connections, and TLS sessions pinned to a single proxy. Cycling
// clients rather than swapping the proxy under one transport is what keeps
// those pools warm across ticks.
type Clients struct {
	clients []*Client
	proxies []string
	next    atomic.Uint64
}

// Len returns the number of clients in the cycle.
func (c *Clients) Len() int {
	return len(c.clients)
}

// Next returns the next client in the cycle. It is safe for concurrent use.
func (c *Clients) Next() *Client {
	if len(c.clients) == 1 {
		return c.clients[0]
	}
	i := c.next.Add(1) - 1

	return c.clients[i%uint64(len(c.clients))]
}

// At returns the client at index i, wrapping around the cycle. Use it when a
// monitor needs a stable client for a given account or session rather than
// round-robin.
func (c *Clients) At(i int) *Client {
	if i < 0 {
		i = -i
	}

	return c.clients[i%len(c.clients)]
}

// Proxy names the exit backing the client at index i, or an empty string for a
// direct connection.
//
// Credentials are stripped: a monitor logging which exit it used is reasonable,
// a monitor holding the pool's credentials is not.
func (c *Clients) Proxy(i int) string {
	if len(c.proxies) == 0 {
		return ""
	}
	if i < 0 {
		i = -i
	}

	return redactProxy(c.proxies[i%len(c.proxies)])
}

// newClients builds one client per assigned proxy, or a single direct client
// when the daemon assigned none.
func newClients(cfg Network) (*Clients, error) {
	proxies := cfg.Proxies
	if len(proxies) == 0 {
		proxies = []string{""}
	}

	clients := make([]*Client, 0, len(proxies))
	for _, proxy := range proxies {
		client, err := newClient(proxy)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}

	return &Clients{
		clients: clients,
		proxies: cfg.Proxies,
	}, nil
}

func newClient(proxy string) (*Client, error) {
	proxyFunc, err := proxyFor(proxy)
	if err != nil {
		return nil, err
	}

	transport := &fhttp.Transport{
		Proxy: proxyFunc,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: true,
		// One transport serves one proxy, so idle connections stay pinned to
		// that exit and survive between ticks.
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	mimicTransport, err := mimic.NewTransport(mimic.TransportOptions{
		Version:   DefaultMimicVersion,
		Brand:     mimic.BrandChrome,
		Platform:  mimic.PlatformMac,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("create mimic transport: %w", err)
	}

	return &Client{Transport: mimicTransport}, nil
}

func proxyFor(proxy string) (func(*fhttp.Request) (*url.URL, error), error) {
	if proxy == "" {
		return nil, nil
	}

	proxyURL, err := url.Parse(proxy)
	if err != nil {
		return nil, fmt.Errorf("parse proxy url: %w", err)
	}
	if proxyURL.Host == "" {
		return nil, fmt.Errorf("proxy url %q has no host", redactProxy(proxy))
	}

	return func(*fhttp.Request) (*url.URL, error) {
		return proxyURL, nil
	}, nil
}

// redactProxy strips credentials from a proxy URL for logs and errors.
func redactProxy(proxy string) string {
	parsed, err := url.Parse(proxy)
	if err != nil || parsed.Host == "" {
		return "<proxy>"
	}
	parsed.User = nil

	return parsed.String()
}
