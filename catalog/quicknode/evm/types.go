package evm

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ChainID string
type Address string
type Hash string

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

type Log struct {
	ChainID     ChainID
	BlockNumber uint64
	BlockHash   Hash
	TxHash      Hash
	TxIndex     uint
	LogIndex    uint
	Address     Address
	Topics      []Hash
	Data        []byte
	Removed     bool
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
