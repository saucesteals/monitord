// Package httpx provides reusable browser-compatible HTTP clients for authored
// monitors.
//
// NewClient uses the host's direct network path. NewProxyClient reads one exact
// monitord secret containing a JSON array of http, https, or socks5 proxy URLs,
// creates a transport per proxy, and selects them in round-robin order. Keeping
// transports separate prevents connections and TLS sessions from drifting
// between exits; redirects remain on the client selected for the request.
//
// Clients are intended to be worker-generation resources. Construct them once
// from a monitor's Start method, using the secrets in monitord.Environment, and
// close idle connections from Stop. Do not build a new proxy pool on every
// polling callback.
package httpx
