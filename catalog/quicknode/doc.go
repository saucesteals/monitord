// Package quicknode provides three explicit QuickNode/EVM integration layers.
//
// Client is a raw JSON-RPC HTTP and WebSocket client. It verifies chain identity
// but deliberately does not imply checkpointing or finality. Callers own its
// lifecycle and any source progress.
//
// Events is a managed confirmed-log source that checkpoints every covered block
// atomically with handler state and events. Wallet is the turnkey monitor for
// confirmed ERC-20, ERC-721, ERC-1155, and non-zero top-level native transfers
// involving one address. Internal calls, trace-derived transfers, balance
// inference, pending records, and non-standard token events are intentionally
// outside Wallet's scope.
//
// Managed monitors use the exact secrets quicknode/websocket-url and, when
// supplied, quicknode/http-url. Wallet also uses quicknode/wallet-address when
// Address is not configured directly. Endpoint credentials are redacted from
// surfaced transport errors.
//
// External notification delivery remains at least once. Reorganizations beyond
// the confirmation window produce correction events for occurrences retained in
// the wallet's bounded canonical journal.
package quicknode
