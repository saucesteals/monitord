package evm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"unicode/utf8"

	"github.com/saucesteals/monitord/catalog/quicknode"
	"golang.org/x/sync/errgroup"
)

const ERC20TransferTopic Hash = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

var ErrERC721Transfer = errors.New("quicknode evm: log is an ERC-721 transfer")

type ERC20Transfer struct {
	Token  Address
	From   Address
	To     Address
	Amount *big.Int
}

type ERC20Metadata struct {
	Token       Address
	Name        string
	Symbol      string
	Decimals    *uint8
	TotalSupply *big.Int
}

// ERC20TransfersTo is the exact server-side filter for standard Transfer logs
// whose indexed recipient is address.
func ERC20TransfersTo(address Address) Logs {
	return Logs{Topics: [][]Hash{{ERC20TransferTopic}, nil, {addressTopic(address)}}}
}

func addressTopic(address Address) Hash {
	value := string(address)
	if len(value) != 42 {
		return ""
	}
	return Hash("0x000000000000000000000000" + value[2:])
}

// DecodeERC20Transfer decodes one standard ERC-20 Transfer log.
func DecodeERC20Transfer(log Log) (ERC20Transfer, error) {
	if len(log.Topics) == 4 && log.Topics[0] == ERC20TransferTopic && len(log.Data) == 0 {
		return ERC20Transfer{}, ErrERC721Transfer
	}
	if len(log.Topics) != 3 || log.Topics[0] != ERC20TransferTopic || len(log.Data) != 32 {
		return ERC20Transfer{}, errors.New("quicknode evm: log is not a standard ERC-20 transfer")
	}
	from, err := topicAddress(log.Topics[1])
	if err != nil {
		return ERC20Transfer{}, err
	}
	to, err := topicAddress(log.Topics[2])
	if err != nil {
		return ERC20Transfer{}, err
	}
	return ERC20Transfer{Token: log.Address, From: from, To: to, Amount: new(big.Int).SetBytes(log.Data)}, nil
}

func topicAddress(topic Hash) (Address, error) {
	value := string(topic)
	if len(value) != 66 || value[2:26] != "000000000000000000000000" {
		return "", errors.New("quicknode evm: indexed address topic has nonzero padding")
	}
	return ParseAddress("0x" + value[26:])
}

type ContractResultError struct{ cause error }

func (e *ContractResultError) Error() string {
	return "quicknode evm: contract returned an unusable result"
}
func (e *ContractResultError) Unwrap() error { return e.cause }

func IsContractResultError(err error) bool {
	var target *ContractResultError
	return errors.As(err, &target)
}

// ERC20Metadata reads standard token metadata from the exact canonical fork
// identified by blockHash. Name, symbol, and decimals are optional in ERC-20;
// unsupported or malformed optional fields are left empty.
func (c *Client) ERC20Metadata(ctx context.Context, token Address, blockHash Hash) (ERC20Metadata, error) {
	result := ERC20Metadata{Token: token}
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		value, err := c.erc20String(ctx, token, blockHash, "0x06fdde03")
		if optionalERC20Result(err) {
			return nil
		}
		result.Name = value
		return err
	})
	group.Go(func() error {
		value, err := c.erc20String(ctx, token, blockHash, "0x95d89b41")
		if optionalERC20Result(err) {
			return nil
		}
		result.Symbol = value
		return err
	})
	group.Go(func() error {
		value, err := c.erc20Uint8(ctx, token, blockHash, "0x313ce567")
		if optionalERC20Result(err) {
			return nil
		}
		result.Decimals = value
		return err
	})
	group.Go(func() error {
		value, err := c.ERC20TotalSupply(ctx, token, blockHash)
		result.TotalSupply = value
		return err
	})
	if err := group.Wait(); err != nil {
		return ERC20Metadata{}, err
	}
	return result, nil
}

func optionalERC20Result(err error) bool { return err != nil && IsContractResultError(err) }

// ERC20TotalSupply reads totalSupply from the exact canonical fork identified
// by blockHash using EIP-1898.
func (c *Client) ERC20TotalSupply(ctx context.Context, token Address, blockHash Hash) (*big.Int, error) {
	data, err := c.contractCall(ctx, token, blockHash, "0x18160ddd")
	if err != nil {
		return nil, err
	}
	if len(data) != 32 {
		return nil, unusableContractResult("invalid totalSupply encoding")
	}
	return new(big.Int).SetBytes(data), nil
}

func (c *Client) erc20String(ctx context.Context, token Address, blockHash Hash, selector string) (string, error) {
	data, err := c.contractCall(ctx, token, blockHash, selector)
	if err != nil {
		return "", err
	}
	var value []byte
	switch {
	case len(data) == 32:
		value = data
	case len(data) >= 64:
		offset := new(big.Int).SetBytes(data[:32])
		if !offset.IsUint64() || offset.Uint64() > uint64(len(data)-32) {
			return "", unusableContractResult("invalid string offset")
		}
		start := int(offset.Uint64())
		size := new(big.Int).SetBytes(data[start : start+32])
		if !size.IsUint64() || size.Uint64() > uint64(len(data)-start-32) {
			return "", unusableContractResult("invalid string length")
		}
		value = data[start+32 : start+32+int(size.Uint64())]
	default:
		return "", unusableContractResult("invalid string encoding")
	}
	value = []byte(strings.TrimSpace(strings.TrimRight(string(value), "\x00")))
	if !utf8.Valid(value) {
		return "", unusableContractResult("string is not UTF-8")
	}
	return string(value), nil
}

func (c *Client) erc20Uint8(ctx context.Context, token Address, blockHash Hash, selector string) (*uint8, error) {
	data, err := c.contractCall(ctx, token, blockHash, selector)
	if err != nil {
		return nil, err
	}
	if len(data) != 32 {
		return nil, unusableContractResult("invalid uint8 encoding")
	}
	value := new(big.Int).SetBytes(data)
	if !value.IsUint64() || value.Uint64() > 255 {
		return nil, unusableContractResult("uint8 result is out of range")
	}
	parsed := uint8(value.Uint64())
	return &parsed, nil
}

func (c *Client) contractCall(ctx context.Context, token Address, blockHash Hash, selector string) ([]byte, error) {
	if _, err := ParseAddress(string(token)); err != nil {
		return nil, err
	}
	if _, err := ParseHash(string(blockHash)); err != nil {
		return nil, err
	}
	var result json.RawMessage
	if err := c.call(ctx, "eth_call", []any{
		map[string]string{"to": string(token), "data": selector},
		map[string]any{"blockHash": blockHash, "requireCanonical": true},
	}, &result); err != nil {
		var rpcErr *quicknode.RPCError
		if errors.As(err, &rpcErr) {
			return nil, &ContractResultError{cause: err}
		}
		return nil, err
	}
	var encoded string
	if err := json.Unmarshal(result, &encoded); err != nil {
		return nil, unusableContractResult("result is not hex data")
	}
	data, err := decodeHex(encoded)
	if err != nil {
		return nil, unusableContractResult("invalid hex result")
	}
	return data, nil
}

func unusableContractResult(reason string) error {
	return &ContractResultError{cause: fmt.Errorf("quicknode evm: %s", reason)}
}
