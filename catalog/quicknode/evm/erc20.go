package evm

import (
	"context"
	"errors"
	"fmt"
	"math/big"
)

const ERC20TransferTopic Hash = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

type ERC20Transfer struct {
	Token  Address
	From   Address
	To     Address
	Amount *big.Int
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

// ERC20TotalSupply reads totalSupply at the log's exact block.
func (c *Client) ERC20TotalSupply(ctx context.Context, token Address, block uint64) (*big.Int, error) {
	if _, err := ParseAddress(string(token)); err != nil {
		return nil, err
	}
	var encoded string
	if err := c.call(ctx, "eth_call", []any{
		map[string]string{"to": string(token), "data": "0x18160ddd"},
		fmt.Sprintf("0x%x", block),
	}, &encoded); err != nil {
		return nil, err
	}
	value, ok := new(big.Int).SetString(encoded, 0)
	if !ok {
		return nil, errors.New("quicknode evm: token returned an invalid total supply")
	}
	return value, nil
}
