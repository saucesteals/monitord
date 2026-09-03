package evm

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

type ChainID string
type Address string
type Hash string
type Quantity string

func ParseQuantity(s string) (Quantity, error) {
	if len(s) < 3 || !strings.HasPrefix(s, "0x") {
		return "", errors.New("quantity must be 0x-prefixed")
	}
	digits := strings.ToLower(s[2:])
	if len(digits) > 1 && digits[0] == '0' {
		return "", errors.New("quantity has a leading zero")
	}
	if _, ok := new(big.Int).SetString(digits, 16); !ok {
		return "", errors.New("quantity is not hexadecimal")
	}
	return Quantity("0x" + digits), nil
}

func (q Quantity) BigInt() (*big.Int, error) {
	parsed, err := ParseQuantity(string(q))
	if err != nil {
		return nil, err
	}
	n, _ := new(big.Int).SetString(string(parsed)[2:], 16)
	return n, nil
}

func (q Quantity) Uint64() (uint64, error) {
	parsed, err := ParseQuantity(string(q))
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(parsed)[2:], 16, 64)
}

func ParseChainID(s string) (ChainID, error) {
	v, err := canonicalQuantity(s)
	if err != nil {
		return "", fmt.Errorf("chain id: %w", err)
	}
	return ChainID(v), nil
}

func ParseAddress(s string) (Address, error) {
	if err := fixedHex(s, 20); err != nil {
		return "", fmt.Errorf("address: %w", err)
	}
	return Address(strings.ToLower(s)), nil
}

func ParseHash(s string) (Hash, error) {
	if err := fixedHex(s, 32); err != nil {
		return "", fmt.Errorf("hash: %w", err)
	}
	return Hash(strings.ToLower(s)), nil
}

func fixedHex(s string, size int) error {
	if len(s) != 2+size*2 || !strings.HasPrefix(s, "0x") {
		return fmt.Errorf("must be 0x-prefixed %d-byte hex", size)
	}
	if _, err := hex.DecodeString(s[2:]); err != nil {
		return errors.New("contains invalid hex")
	}
	return nil
}

func canonicalQuantity(s string) (string, error) {
	if len(s) < 3 || !strings.HasPrefix(s, "0x") {
		return "", errors.New("must be a 0x-prefixed quantity")
	}
	digits := strings.ToLower(s[2:])
	if len(digits) > 1 && digits[0] == '0' {
		return "", errors.New("has a leading zero")
	}
	if _, err := strconv.ParseUint(digits, 16, 64); err != nil {
		return "", errors.New("is not a uint64 hex quantity")
	}
	return "0x" + digits, nil
}

type Transaction struct {
	ChainID     ChainID  `json:"chain_id"`
	BlockNumber uint64   `json:"block_number"`
	BlockHash   Hash     `json:"block_hash"`
	Hash        Hash     `json:"hash"`
	Index       uint     `json:"index"`
	From        Address  `json:"from"`
	To          *Address `json:"to,omitempty"`
	Nonce       uint64   `json:"nonce"`
	Value       Quantity `json:"value"`
	Input       []byte   `json:"input,omitempty"`
}

func (t Transaction) ID() string {
	return fmt.Sprintf("evm:%s:transaction:%s:%s", t.ChainID, t.BlockHash, t.Hash)
}

func (t Transaction) Clone() Transaction {
	t.Input = append([]byte(nil), t.Input...)
	if t.To != nil {
		to := *t.To
		t.To = &to
	}
	return t
}

type Block struct {
	ChainID      ChainID       `json:"chain_id"`
	Number       uint64        `json:"number"`
	Hash         Hash          `json:"hash"`
	ParentHash   Hash          `json:"parent_hash"`
	Timestamp    uint64        `json:"timestamp"`
	Transactions []Transaction `json:"transactions,omitempty"`
	Removed      bool          `json:"removed,omitempty"`
	Confirmed    bool          `json:"confirmed,omitempty"`
}

func (b Block) ID() string {
	return fmt.Sprintf("evm:%s:block:%s", b.ChainID, b.Hash)
}

func (b Block) Clone() Block {
	transactions := b.Transactions
	b.Transactions = make([]Transaction, len(transactions))
	for i := range transactions {
		b.Transactions[i] = transactions[i].Clone()
	}
	return b
}

type Receipt struct {
	ChainID           ChainID  `json:"chain_id"`
	BlockNumber       uint64   `json:"block_number"`
	BlockHash         Hash     `json:"block_hash"`
	TxHash            Hash     `json:"transaction_hash"`
	TxIndex           uint     `json:"transaction_index"`
	Success           bool     `json:"success"`
	ContractAddress   *Address `json:"contract_address,omitempty"`
	GasUsed           Quantity `json:"gas_used"`
	EffectiveGasPrice Quantity `json:"effective_gas_price,omitempty"`
	Logs              []Log    `json:"logs,omitempty"`
}

func (r Receipt) Clone() Receipt {
	if r.ContractAddress != nil {
		address := *r.ContractAddress
		r.ContractAddress = &address
	}
	logs := r.Logs
	r.Logs = make([]Log, len(logs))
	for i := range logs {
		r.Logs[i] = logs[i].Clone()
	}
	return r
}

type Account struct {
	Address   Address  `json:"address"`
	BlockHash Hash     `json:"block_hash"`
	Balance   Quantity `json:"balance"`
	Nonce     uint64   `json:"nonce"`
	Code      []byte   `json:"code,omitempty"`
}

func (a Account) IsEOA() bool { return len(a.Code) == 0 }

func (a Account) Clone() Account {
	a.Code = append([]byte(nil), a.Code...)
	return a
}

type Log struct {
	ChainID     ChainID `json:"chain_id"`
	BlockNumber uint64  `json:"block_number"`
	BlockHash   Hash    `json:"block_hash"`
	TxHash      Hash    `json:"transaction_hash"`
	TxIndex     uint    `json:"transaction_index"`
	LogIndex    uint    `json:"log_index"`
	Address     Address `json:"address"`
	Topics      []Hash  `json:"topics"`
	Data        []byte  `json:"data"`
	Removed     bool    `json:"removed,omitempty"`
	Confirmed   bool    `json:"confirmed,omitempty"`
}

func (l Log) ID() string {
	return fmt.Sprintf("evm:%s:log:%s:%s:%d", l.ChainID, l.BlockHash, l.TxHash, l.LogIndex)
}
func (l Log) Clone() Log {
	l.Topics = append([]Hash(nil), l.Topics...)
	l.Data = append([]byte(nil), l.Data...)
	return l
}

type Logs struct {
	Addresses []Address `json:"addresses,omitempty"`
	Topics    [][]Hash  `json:"topics,omitempty"`
}

func (f Logs) Clone() Logs {
	clone := Logs{Addresses: append([]Address(nil), f.Addresses...)}
	if len(f.Topics) > 0 {
		clone.Topics = make([][]Hash, len(f.Topics))
		for i := range f.Topics {
			clone.Topics[i] = append([]Hash(nil), f.Topics[i]...)
		}
	}
	return clone
}

func (f Logs) Validate() error {
	hasFilter := len(f.Addresses) > 0
	for _, a := range f.Addresses {
		if _, err := ParseAddress(string(a)); err != nil {
			return err
		}
	}
	for _, set := range f.Topics {
		hasFilter = hasFilter || len(set) > 0
		for _, h := range set {
			if _, err := ParseHash(string(h)); err != nil {
				return fmt.Errorf("topic: %w", err)
			}
		}
	}
	if !hasFilter {
		return errors.New("log filter requires an address or topic")
	}
	return nil
}

func (f Logs) rpcArgs() map[string]any {
	args := map[string]any{}
	if len(f.Addresses) == 1 {
		args["address"] = f.Addresses[0]
	} else if len(f.Addresses) > 1 {
		args["address"] = f.Addresses
	}
	if len(f.Topics) > 0 {
		topics := make([]any, len(f.Topics))
		for i, choices := range f.Topics {
			switch len(choices) {
			case 0:
				topics[i] = nil
			case 1:
				topics[i] = choices[0]
			default:
				topics[i] = choices
			}
		}
		args["topics"] = topics
	}
	return args
}
