// Package evm provides Ethereum-compatible access on top of QuickNode.
//
// Client verifies EVM chain identity and exposes EVM subscriptions. Events processes confirmed
// logs and checkpoints source progress atomically with its handler. Wallet
// monitors confirmed ERC-20, ERC-721, ERC-1155, and non-zero top-level native
// transfers involving one explicit address.
//
// Managed monitors default to quicknode/ethereum-mainnet-http-url. Custom EVM
// networks provide HTTPSecret or HTTPURL and should also provide ExpectedChainID
// and an explicit confirmation depth.
package evm
