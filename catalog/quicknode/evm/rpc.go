package evm

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// ErrReceiptUnavailable reports head-to-RPC propagation lag for a mined
// transaction whose receipt endpoint still returns null.
var ErrReceiptUnavailable = errors.New("quicknode evm: transaction receipt is not available")

// ErrBlockUnavailable reports head-to-RPC propagation lag for an announced
// block whose HTTP endpoint still returns null.
var ErrBlockUnavailable = errors.New("quicknode evm: block is not available")

type rpcBlock struct {
	Number       string           `json:"number"`
	Hash         Hash             `json:"hash"`
	ParentHash   Hash             `json:"parentHash"`
	Timestamp    string           `json:"timestamp"`
	Transactions []rpcTransaction `json:"transactions"`
}
type rpcBlockHeader struct {
	Number     string `json:"number"`
	Hash       Hash   `json:"hash"`
	ParentHash Hash   `json:"parentHash"`
}
type rpcTransaction struct {
	BlockNumber      string   `json:"blockNumber"`
	BlockHash        Hash     `json:"blockHash"`
	Hash             Hash     `json:"hash"`
	TransactionIndex string   `json:"transactionIndex"`
	From             Address  `json:"from"`
	To               *Address `json:"to"`
	Nonce            string   `json:"nonce"`
	Value            string   `json:"value"`
	Input            string   `json:"input"`
}
type rpcReceipt struct {
	BlockNumber       string   `json:"blockNumber"`
	BlockHash         Hash     `json:"blockHash"`
	TransactionHash   Hash     `json:"transactionHash"`
	TransactionIndex  string   `json:"transactionIndex"`
	Status            string   `json:"status"`
	ContractAddress   *Address `json:"contractAddress"`
	GasUsed           string   `json:"gasUsed"`
	EffectiveGasPrice string   `json:"effectiveGasPrice"`
	Logs              []rpcLog `json:"logs"`
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
func (c *Client) blockByNumber(ctx context.Context, n uint64) (rpcBlockHeader, error) {
	var block rpcBlockHeader
	err := c.call(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", n), false}, &block)
	if err == nil {
		number, parseErr := parseUintQuantity(block.Number)
		if parseErr != nil || number != n {
			return rpcBlockHeader{}, errors.New("quicknode: invalid block response")
		}
		if _, parseErr = ParseHash(string(block.Hash)); parseErr != nil {
			return rpcBlockHeader{}, parseErr
		}
		if _, parseErr = ParseHash(string(block.ParentHash)); parseErr != nil {
			return rpcBlockHeader{}, parseErr
		}
		return block, nil
	}
	return rpcBlockHeader{}, err
}

// BlockByNumber returns the canonical block at number with full transactions.
func (c *Client) BlockByNumber(ctx context.Context, number uint64) (Block, error) {
	return retryPropagation(ctx, ErrBlockUnavailable, func() (Block, error) {
		return c.blockByNumberFull(ctx, number)
	})
}

func (c *Client) blockByNumberFull(ctx context.Context, number uint64) (Block, error) {
	var raw rpcBlock
	if err := c.callBlock(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", number), true}, &raw); err != nil {
		return Block{}, err
	}
	block, err := decodeRPCBlock(raw, c.ChainID())
	if err != nil {
		return Block{}, err
	}
	if block.Number != number {
		return Block{}, errors.New("quicknode: block number does not match request")
	}
	return block, nil
}

// BlockByHash returns a block with full transactions from its immutable hash.
func (c *Client) BlockByHash(ctx context.Context, hash Hash) (Block, error) {
	if _, err := ParseHash(string(hash)); err != nil {
		return Block{}, err
	}
	return retryPropagation(ctx, ErrBlockUnavailable, func() (Block, error) {
		return c.blockByHashFull(ctx, hash)
	})
}

func (c *Client) blockByHashFull(ctx context.Context, hash Hash) (Block, error) {
	var raw rpcBlock
	if err := c.callBlock(ctx, "eth_getBlockByHash", []any{hash, true}, &raw); err != nil {
		return Block{}, err
	}
	block, err := decodeRPCBlock(raw, c.ChainID())
	if err != nil {
		return Block{}, err
	}
	if block.Hash != hash {
		return Block{}, errors.New("quicknode: block hash does not match request")
	}
	return block, nil
}

func (c *Client) callBlock(ctx context.Context, method string, params []any, out *rpcBlock) error {
	var payload json.RawMessage
	if err := c.call(ctx, method, params, &payload); err != nil {
		return err
	}
	if len(payload) == 0 || bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
		return ErrBlockUnavailable
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return errors.New("quicknode: invalid block response")
	}
	return nil
}

func (c *Client) blocksByNumber(ctx context.Context, from, to uint64, concurrency int) ([]Block, error) {
	if to < from {
		return nil, errors.New("quicknode: invalid block range")
	}
	if concurrency < 1 {
		return nil, errors.New("quicknode: block fetch concurrency must be positive")
	}
	blocks := make([]Block, to-from+1)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for i := range blocks {
		i := i
		group.Go(func() error {
			block, err := c.BlockByNumber(groupCtx, from+uint64(i))
			if err == nil {
				blocks[i] = block
			}
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	for i := 1; i < len(blocks); i++ {
		if blocks[i].ParentHash != blocks[i-1].Hash {
			return nil, errors.New("quicknode: block range is not a canonical chain")
		}
	}
	return blocks, nil
}

// TransactionReceipt returns the receipt for hash and validates all embedded
// block, transaction, address, quantity, and log fields.
func (c *Client) TransactionReceipt(ctx context.Context, hash Hash) (Receipt, error) {
	if _, err := ParseHash(string(hash)); err != nil {
		return Receipt{}, err
	}
	var payload json.RawMessage
	if err := c.call(ctx, "eth_getTransactionReceipt", []any{hash}, &payload); err != nil {
		return Receipt{}, err
	}
	if len(payload) == 0 || bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
		return Receipt{}, ErrReceiptUnavailable
	}
	var raw rpcReceipt
	if err := json.Unmarshal(payload, &raw); err != nil {
		return Receipt{}, errors.New("quicknode: invalid transaction receipt")
	}
	receipt, err := decodeRPCReceipt(raw, c.ChainID())
	if err != nil {
		return Receipt{}, err
	}
	if receipt.TxHash != hash {
		return Receipt{}, errors.New("quicknode: receipt hash does not match request")
	}
	return receipt, nil
}

// ReceiptFor returns tx's receipt and rejects a response from another block or
// transaction position. This is the preferred helper for fork-sensitive logic.
func (c *Client) ReceiptFor(ctx context.Context, tx Transaction) (Receipt, error) {
	if tx.ChainID != c.ChainID() {
		return Receipt{}, errors.New("quicknode: transaction belongs to another chain")
	}
	receipt, err := retryPropagation(ctx, ErrReceiptUnavailable, func() (Receipt, error) {
		return c.TransactionReceipt(ctx, tx.Hash)
	})
	if err != nil {
		return Receipt{}, err
	}
	if receipt.BlockHash != tx.BlockHash || receipt.BlockNumber != tx.BlockNumber || receipt.TxIndex != tx.Index {
		return Receipt{}, errors.New("quicknode: receipt does not match transaction block")
	}
	return receipt, nil
}

func retryPropagation[T any](ctx context.Context, unavailable error, fetch func() (T, error)) (T, error) {
	var value T
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		value, err = fetch()
		if !errors.Is(err, unavailable) || attempt == 4 {
			return value, err
		}
		delay := min(50*time.Millisecond<<attempt, 500*time.Millisecond)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return value, ctx.Err()
		case <-timer.C:
		}
	}
	return value, err
}

// AccountAt reads balance, nonce, and code from the exact canonical block hash.
func (c *Client) AccountAt(ctx context.Context, address Address, blockHash Hash) (Account, error) {
	if _, err := ParseAddress(string(address)); err != nil {
		return Account{}, err
	}
	if _, err := ParseHash(string(blockHash)); err != nil {
		return Account{}, err
	}
	block := map[string]any{"blockHash": blockHash, "requireCanonical": true}
	var balanceRaw, nonceRaw, codeRaw string
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return c.call(groupCtx, "eth_getBalance", []any{address, block}, &balanceRaw) })
	group.Go(func() error { return c.call(groupCtx, "eth_getTransactionCount", []any{address, block}, &nonceRaw) })
	group.Go(func() error { return c.call(groupCtx, "eth_getCode", []any{address, block}, &codeRaw) })
	if err := group.Wait(); err != nil {
		return Account{}, err
	}
	balance, err := ParseQuantity(balanceRaw)
	if err != nil {
		return Account{}, err
	}
	nonce, err := parseUintQuantity(nonceRaw)
	if err != nil {
		return Account{}, err
	}
	code, err := decodeHex(codeRaw)
	if err != nil {
		return Account{}, err
	}
	return Account{Address: address, BlockHash: blockHash, Balance: balance, Nonce: nonce, Code: code}, nil
}

func decodeRPCBlock(raw rpcBlock, chain ChainID) (Block, error) {
	number, err := parseUintQuantity(raw.Number)
	if err != nil {
		return Block{}, err
	}
	timestamp, err := parseUintQuantity(raw.Timestamp)
	if err != nil {
		return Block{}, err
	}
	if _, err = ParseHash(string(raw.Hash)); err != nil {
		return Block{}, err
	}
	if _, err = ParseHash(string(raw.ParentHash)); err != nil {
		return Block{}, err
	}
	block := Block{ChainID: chain, Number: number, Hash: raw.Hash, ParentHash: raw.ParentHash, Timestamp: timestamp, Transactions: make([]Transaction, len(raw.Transactions))}
	for i := range raw.Transactions {
		tx, decodeErr := decodeRPCTransaction(raw.Transactions[i], chain)
		if decodeErr != nil {
			return Block{}, decodeErr
		}
		if tx.BlockNumber != number || tx.BlockHash != raw.Hash || tx.Index != uint(i) {
			return Block{}, errors.New("quicknode: transaction does not belong to containing block")
		}
		block.Transactions[i] = tx
	}
	return block, nil
}

func decodeRPCTransaction(raw rpcTransaction, chain ChainID) (Transaction, error) {
	blockNumber, err := parseUintQuantity(raw.BlockNumber)
	if err != nil {
		return Transaction{}, err
	}
	index, err := parseUintQuantity(raw.TransactionIndex)
	if err != nil {
		return Transaction{}, err
	}
	nonce, err := parseUintQuantity(raw.Nonce)
	if err != nil {
		return Transaction{}, err
	}
	value, err := ParseQuantity(raw.Value)
	if err != nil {
		return Transaction{}, err
	}
	input, err := decodeHex(raw.Input)
	if err != nil {
		return Transaction{}, err
	}
	if _, err = ParseHash(string(raw.BlockHash)); err != nil {
		return Transaction{}, err
	}
	if _, err = ParseHash(string(raw.Hash)); err != nil {
		return Transaction{}, err
	}
	if _, err = ParseAddress(string(raw.From)); err != nil {
		return Transaction{}, err
	}
	if raw.To != nil {
		if _, err = ParseAddress(string(*raw.To)); err != nil {
			return Transaction{}, err
		}
	}
	return Transaction{ChainID: chain, BlockNumber: blockNumber, BlockHash: raw.BlockHash, Hash: raw.Hash, Index: uint(index), From: raw.From, To: raw.To, Nonce: nonce, Value: value, Input: input}, nil
}

func decodeRPCReceipt(raw rpcReceipt, chain ChainID) (Receipt, error) {
	blockNumber, err := parseUintQuantity(raw.BlockNumber)
	if err != nil {
		return Receipt{}, err
	}
	index, err := parseUintQuantity(raw.TransactionIndex)
	if err != nil {
		return Receipt{}, err
	}
	status, err := parseUintQuantity(raw.Status)
	if err != nil || status > 1 {
		return Receipt{}, errors.New("quicknode: invalid receipt status")
	}
	gasUsed, err := ParseQuantity(raw.GasUsed)
	if err != nil {
		return Receipt{}, err
	}
	var gasPrice Quantity
	if raw.EffectiveGasPrice != "" {
		gasPrice, err = ParseQuantity(raw.EffectiveGasPrice)
		if err != nil {
			return Receipt{}, err
		}
	}
	if _, err = ParseHash(string(raw.BlockHash)); err != nil {
		return Receipt{}, err
	}
	if _, err = ParseHash(string(raw.TransactionHash)); err != nil {
		return Receipt{}, err
	}
	if raw.ContractAddress != nil {
		if _, err = ParseAddress(string(*raw.ContractAddress)); err != nil {
			return Receipt{}, err
		}
	}
	receipt := Receipt{ChainID: chain, BlockNumber: blockNumber, BlockHash: raw.BlockHash, TxHash: raw.TransactionHash, TxIndex: uint(index), Success: status == 1, ContractAddress: raw.ContractAddress, GasUsed: gasUsed, EffectiveGasPrice: gasPrice, Logs: make([]Log, len(raw.Logs))}
	for i := range raw.Logs {
		log, decodeErr := decodeRPCLog(raw.Logs[i], chain)
		if decodeErr != nil {
			return Receipt{}, decodeErr
		}
		if log.BlockNumber != blockNumber || log.BlockHash != raw.BlockHash || log.TxHash != raw.TransactionHash || log.TxIndex != uint(index) {
			return Receipt{}, errors.New("quicknode: receipt log does not belong to receipt")
		}
		receipt.Logs[i] = log
	}
	return receipt, nil
}

// IsCanonicalBlock reports whether hash is currently canonical at number.
func (c *Client) IsCanonicalBlock(ctx context.Context, number uint64, hash Hash) (bool, error) {
	if _, err := ParseHash(string(hash)); err != nil {
		return false, err
	}
	block, err := c.blockByNumber(ctx, number)
	if err != nil {
		return false, err
	}
	return block.Hash == hash, nil
}
func (c *Client) logs(ctx context.Context, f Logs, from, to uint64) ([]Log, error) {
	arg := map[string]any{"fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", to)}
	return c.logsWithArg(ctx, f, arg)
}
func (c *Client) logsWithArg(ctx context.Context, f Logs, arg map[string]any) ([]Log, error) {
	for key, value := range f.rpcArgs() {
		arg[key] = value
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
