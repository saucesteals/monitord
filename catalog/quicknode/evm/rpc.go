package evm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type rpcBlock struct {
	Number       string           `json:"number"`
	Hash         Hash             `json:"hash"`
	ParentHash   Hash             `json:"parentHash"`
	Transactions []rpcTransaction `json:"transactions"`
}
type rpcBlockWire struct {
	Number       string          `json:"number"`
	Hash         Hash            `json:"hash"`
	ParentHash   Hash            `json:"parentHash"`
	Transactions json.RawMessage `json:"transactions"`
}
type rpcTransaction struct {
	Hash             Hash     `json:"hash"`
	From             Address  `json:"from"`
	To               *Address `json:"to"`
	Value            string   `json:"value"`
	TransactionIndex string   `json:"transactionIndex"`
}
type rpcReceipt struct {
	Status          string   `json:"status"`
	ContractAddress *Address `json:"contractAddress"`
}
type rpcLog struct {
	BlockNumber      string  `json:"blockNumber"`
	BlockHash        Hash    `json:"blockHash"`
	TransactionHash  Hash    `json:"transactionHash"`
	TransactionIndex string  `json:"transactionIndex"`
	LogIndex         string  `json:"logIndex"`
	Address          Address `json:"address"`
	Topics           []Hash  `json:"topics"`
	Data             string  `json:"data"`
	Removed          bool    `json:"removed"`
}

func (c *Client) blockNumber(ctx context.Context) (uint64, error) {
	var q string
	if err := c.call(ctx, "eth_blockNumber", []any{}, &q); err != nil {
		return 0, err
	}
	return parseUintQuantity(q)
}
func (c *Client) blockByNumber(ctx context.Context, n uint64, full bool) (rpcBlock, error) {
	var wire rpcBlockWire
	err := c.call(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", n), full}, &wire)
	if err == nil {
		number, parseErr := parseUintQuantity(wire.Number)
		if parseErr != nil || number != n {
			return rpcBlock{}, errors.New("quicknode: invalid block response")
		}
		if _, parseErr = ParseHash(string(wire.Hash)); parseErr != nil {
			return rpcBlock{}, parseErr
		}
		if _, parseErr = ParseHash(string(wire.ParentHash)); parseErr != nil {
			return rpcBlock{}, parseErr
		}
		b := rpcBlock{Number: wire.Number, Hash: wire.Hash, ParentHash: wire.ParentHash}
		if full && json.Unmarshal(wire.Transactions, &b.Transactions) != nil {
			return rpcBlock{}, errors.New("quicknode: invalid full block transactions")
		}
		return b, nil
	}
	return rpcBlock{}, err
}
func (c *Client) logs(ctx context.Context, f Logs, from, to uint64) ([]Log, error) {
	arg := map[string]any{"fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", to)}
	if len(f.Addresses) > 0 {
		arg["address"] = f.Addresses
	}
	if len(f.Topics) > 0 {
		arg["topics"] = f.Topics
	}
	var raw []rpcLog
	if err := c.call(ctx, "eth_getLogs", []any{arg}, &raw); err != nil {
		return nil, err
	}
	out := make([]Log, 0, len(raw))
	for _, r := range raw {
		log, e := decodeRPCLog(r, c.ChainID())
		if e != nil {
			return nil, e
		}
		out = append(out, log)
	}
	return out, nil
}
func (c *Client) transactionReceipt(ctx context.Context, hash Hash) (rpcReceipt, error) {
	var receipt rpcReceipt
	if err := c.call(ctx, "eth_getTransactionReceipt", []any{hash}, &receipt); err != nil {
		return rpcReceipt{}, err
	}
	status, err := parseUintQuantity(receipt.Status)
	if err != nil || status > 1 {
		return rpcReceipt{}, errors.New("quicknode evm: invalid transaction receipt status")
	}
	if receipt.ContractAddress != nil {
		if _, err := ParseAddress(string(*receipt.ContractAddress)); err != nil {
			return rpcReceipt{}, err
		}
	}
	return receipt, nil
}
func decodeRPCLog(r rpcLog, chain ChainID) (Log, error) {
	if _, e := ParseHash(string(r.BlockHash)); e != nil {
		return Log{}, e
	}
	if _, e := ParseHash(string(r.TransactionHash)); e != nil {
		return Log{}, e
	}
	if _, e := ParseAddress(string(r.Address)); e != nil {
		return Log{}, e
	}
	for _, topic := range r.Topics {
		if _, e := ParseHash(string(topic)); e != nil {
			return Log{}, e
		}
	}
	bn, e := parseUintQuantity(r.BlockNumber)
	if e != nil {
		return Log{}, e
	}
	ti, e := parseUintQuantity(r.TransactionIndex)
	if e != nil {
		return Log{}, e
	}
	li, e := parseUintQuantity(r.LogIndex)
	if e != nil {
		return Log{}, e
	}
	data, e := decodeHex(r.Data)
	if e != nil {
		return Log{}, e
	}
	return Log{ChainID: chain, BlockNumber: bn, BlockHash: r.BlockHash, TxHash: r.TransactionHash, TxIndex: uint(ti), LogIndex: uint(li), Address: r.Address, Topics: append([]Hash(nil), r.Topics...), Data: data, Removed: r.Removed}, nil
}
func parseUintQuantity(q string) (uint64, error) {
	c, err := canonicalQuantity(q)
	if err != nil {
		return 0, err
	}
	var n uint64
	_, err = fmt.Sscanf(c, "0x%x", &n)
	return n, err
}
func decodeHex(s string) ([]byte, error) {
	if !strings.HasPrefix(s, "0x") {
		return nil, errors.New("hex data lacks prefix")
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, errors.New("invalid hex data")
	}
	return b, nil
}
