package evm

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/saucesteals/monitord"
	"golang.org/x/crypto/sha3"
)

func (w Wallet) blockTransfers(ctx context.Context, c *Client, wallet Address, kinds TransferKinds, b rpcBlock) ([]Transfer, error) {
	out := []Transfer{}
	blockNumber, err := parseUintQuantity(b.Number)
	if err != nil {
		return nil, err
	}
	if kinds&NativeTransactions != 0 {
		for _, tx := range b.Transactions {
			if _, err = ParseHash(string(tx.Hash)); err != nil {
				return nil, err
			}
			if _, err = ParseAddress(string(tx.From)); err != nil {
				return nil, err
			}
			if tx.To != nil {
				if _, err = ParseAddress(string(*tx.To)); err != nil {
					return nil, err
				}
			}
			value, err := quantityDecimal(tx.Value)
			if err != nil {
				return nil, err
			}
			if value == "0" {
				continue
			}
			to := Address("")
			if tx.To != nil {
				to = *tx.To
			}
			if equalAddress(tx.From, wallet) || equalAddress(to, wallet) {
				receipt, err := c.transactionReceipt(ctx, tx.Hash)
				if err != nil {
					return nil, err
				}
				if receipt.Status == "0x0" {
					continue
				}
				if tx.To == nil && receipt.ContractAddress != nil {
					to = *receipt.ContractAddress
				}
				idx, err := parseUintQuantity(tx.TransactionIndex)
				if err != nil {
					return nil, err
				}
				out = append(out, Transfer{ChainID: c.ChainID(), Kind: Native, BlockNumber: blockNumber, BlockHash: b.Hash, TxHash: tx.Hash, TxIndex: uint(idx), From: tx.From, To: to, Amount: value})
			}
		}
	}
	if kinds&TokenTransfers != 0 {
		logs, err := c.logs(ctx, Logs{Topics: [][]Hash{{transferTopic, transferSingleTopic, transferBatchTopic}}}, blockNumber, blockNumber)
		if err != nil {
			return nil, err
		}
		for _, l := range logs {
			transfers, err := decodeTransferLog(c.ChainID(), l, wallet)
			if err != nil {
				return nil, err
			}
			out = append(out, transfers...)
		}
	}
	return out, nil
}

var (
	transferTopic       = eventTopic("Transfer(address,address,uint256)")
	transferSingleTopic = eventTopic("TransferSingle(address,address,address,uint256,uint256)")
	transferBatchTopic  = eventTopic("TransferBatch(address,address,address,uint256[],uint256[])")
)

func eventTopic(signature string) Hash {
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write([]byte(signature))
	return Hash("0x" + hex.EncodeToString(h.Sum(nil)))
}

func decodeTransferLog(chain ChainID, l Log, wallet Address) ([]Transfer, error) {
	if len(l.Topics) < 3 {
		return nil, nil
	}
	fromIndex, toIndex := 1, 2
	if l.Topics[0] == transferSingleTopic || l.Topics[0] == transferBatchTopic {
		if len(l.Topics) < 4 {
			return nil, errors.New("quicknode: malformed ERC-1155 topics")
		}
		fromIndex, toIndex = 2, 3
	} else if l.Topics[0] != transferTopic {
		return nil, nil
	}
	from, err := topicAddress(l.Topics[fromIndex])
	if err != nil {
		return nil, err
	}
	to, err := topicAddress(l.Topics[toIndex])
	if err != nil {
		return nil, err
	}
	if !equalAddress(from, wallet) && !equalAddress(to, wallet) {
		return nil, nil
	}
	base := Transfer{ChainID: chain, BlockNumber: l.BlockNumber, BlockHash: l.BlockHash, TxHash: l.TxHash, TxIndex: l.TxIndex, From: from, To: to, Contract: l.Address, Removed: l.Removed}
	idx := l.LogIndex
	base.LogIndex = &idx
	switch l.Topics[0] {
	case transferTopic:
		if len(l.Topics) >= 4 {
			base.Kind = ERC721
			base.TokenID = hexInteger(string(l.Topics[3])[2:])
			base.Amount = "1"
		} else {
			base.Kind = ERC20
			base.Amount = new(big.Int).SetBytes(l.Data).String()
		}
		return []Transfer{base}, nil
	case transferSingleTopic:
		if len(l.Data) != 64 {
			return nil, errors.New("quicknode: malformed ERC-1155 TransferSingle data")
		}
		base.Kind = ERC1155
		base.TokenID = new(big.Int).SetBytes(l.Data[:32]).String()
		base.Amount = new(big.Int).SetBytes(l.Data[32:]).String()
		return []Transfer{base}, nil
	case transferBatchTopic:
		ids, values, err := decodeABIUintArrays(l.Data)
		if err != nil {
			return nil, err
		}
		if len(ids) != len(values) {
			return nil, errors.New("quicknode: ERC-1155 batch array lengths differ")
		}
		out := make([]Transfer, len(ids))
		for i := range ids {
			tr := base
			tr.Kind = ERC1155
			tr.TokenID = ids[i]
			tr.Amount = values[i]
			bi := uint(i)
			tr.BatchIndex = &bi
			out[i] = tr
		}
		return out, nil
	default:
		return nil, nil
	}
}
func hexInteger(s string) string { n := new(big.Int); n.SetString(s, 16); return n.String() }
func decodeABIUintArrays(data []byte) ([]string, []string, error) {
	if len(data) < 64 || len(data)%32 != 0 {
		return nil, nil, errors.New("quicknode: malformed ABI batch data")
	}
	readOffset := func(word []byte) (int, error) {
		n := new(big.Int).SetBytes(word)
		if !n.IsInt64() {
			return 0, errors.New("quicknode: ABI offset overflow")
		}
		v := int(n.Int64())
		if v < 0 || v+32 > len(data) || v%32 != 0 {
			return 0, errors.New("quicknode: invalid ABI offset")
		}
		return v, nil
	}
	decode := func(off int) ([]string, error) {
		count := new(big.Int).SetBytes(data[off : off+32])
		if !count.IsInt64() {
			return nil, errors.New("quicknode: ABI array too large")
		}
		n := int(count.Int64())
		if n < 0 || n > (len(data)-off-32)/32 {
			return nil, errors.New("quicknode: truncated ABI array")
		}
		out := make([]string, n)
		for i := range out {
			out[i] = new(big.Int).SetBytes(data[off+32+i*32 : off+64+i*32]).String()
		}
		return out, nil
	}
	a, e := readOffset(data[:32])
	if e != nil {
		return nil, nil, e
	}
	b, e := readOffset(data[32:64])
	if e != nil {
		return nil, nil, e
	}
	ids, e := decode(a)
	if e != nil {
		return nil, nil, e
	}
	values, e := decode(b)
	return ids, values, e
}
func topicAddress(h Hash) (Address, error) {
	s := string(h)
	if len(s) != 66 {
		return "", errors.New("quicknode: invalid address topic")
	}
	return ParseAddress("0x" + s[len(s)-40:])
}
func equalAddress(a, b Address) bool { return strings.EqualFold(string(a), string(b)) }
func quantityDecimal(q string) (string, error) {
	if _, err := canonicalQuantity(q); err != nil {
		return "", err
	}
	n := new(big.Int)
	if _, ok := n.SetString(q[2:], 16); !ok {
		return "", errors.New("invalid quantity")
	}
	return n.String(), nil
}
func (w Wallet) mapEvent(t Transfer) monitord.Event {
	return w.mapEventFor(t, w.Address)
}
func (w Wallet) mapEventFor(t Transfer, watched Address) monitord.Event {
	if w.Map != nil {
		return w.Map(t)
	}
	direction := "received"
	if equalAddress(t.From, watched) {
		direction = "sent"
	}
	return monitord.Event{
		ID: t.ID(), Title: "Wallet transfer", Body: fmt.Sprintf("%s %s units", direction, t.Amount),
		Data: map[string]string{"from": string(t.From), "to": string(t.To), "chain": string(t.ChainID), "transaction": string(t.TxHash)},
	}
}
