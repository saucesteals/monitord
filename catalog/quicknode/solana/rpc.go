package solana

import (
	"context"
	"encoding/json"
	"errors"
)

type Encoding string

const (
	Base58     Encoding = "base58"
	Base64     Encoding = "base64"
	Base64Zstd Encoding = "base64+zstd"
	Binary     Encoding = "binary"
	JSONParsed Encoding = "jsonParsed"
)

func (e Encoding) validate() error {
	switch e {
	case Base58, Base64, Base64Zstd, Binary, JSONParsed:
		return nil
	default:
		return errors.New("solana account encoding is invalid")
	}
}

type Context struct {
	Slot Slot `json:"slot"`
}

type BlockOptions struct {
	Commitment                     Commitment
	TransactionDetails             string
	Rewards                        *bool
	MaxSupportedTransactionVersion *uint8
}

type TransactionOptions struct {
	Commitment                     Commitment
	MaxSupportedTransactionVersion *uint8
}

type SignaturesOptions struct {
	Commitment     Commitment
	Before         Signature
	Until          Signature
	Limit          int
	MinContextSlot *Slot
}

type SignatureInfo struct {
	Signature          Signature       `json:"signature"`
	Slot               Slot            `json:"slot"`
	Err                json.RawMessage `json:"err"`
	Memo               *string         `json:"memo"`
	BlockTime          *int64          `json:"blockTime"`
	ConfirmationStatus *Commitment     `json:"confirmationStatus"`
}

type DataSlice struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

type AccountOptions struct {
	Commitment     Commitment
	Encoding       Encoding
	DataSlice      *DataSlice
	MinContextSlot *Slot
}

type AccountInfo struct {
	Context Context         `json:"context"`
	Value   json.RawMessage `json:"value"`
}

type ProgramAccountsOptions struct {
	Commitment     Commitment
	Encoding       Encoding
	DataSlice      *DataSlice
	MinContextSlot *Slot
	Filters        []json.RawMessage
}

type ProgramAccount struct {
	PublicKey PublicKey       `json:"pubkey"`
	Account   json.RawMessage `json:"account"`
}

func (c *Client) GetSlot(ctx context.Context) (Slot, error) {
	var slot Slot
	err := c.call(ctx, "getSlot", []any{map[string]any{"commitment": c.commitment}}, &slot)
	return slot, err
}

func (c *Client) GetBlock(ctx context.Context, slot Slot, opts BlockOptions) (json.RawMessage, error) {
	commitment, err := c.selectCommitment(opts.Commitment)
	if err != nil {
		return nil, err
	}
	if commitment == Processed {
		return nil, errors.New("solana getBlock does not support processed commitment")
	}
	details := opts.TransactionDetails
	if details == "" {
		details = "full"
	}
	switch details {
	case "full", "accounts", "signatures", "none":
	default:
		return nil, errors.New("solana block transaction details are invalid")
	}
	config := map[string]any{
		"commitment":         commitment,
		"encoding":           "json",
		"transactionDetails": details,
	}
	if opts.Rewards != nil {
		config["rewards"] = *opts.Rewards
	}
	config["maxSupportedTransactionVersion"] = transactionVersion(opts.MaxSupportedTransactionVersion)
	var result json.RawMessage
	err = c.call(ctx, "getBlock", []any{slot, config}, &result)
	return result, err
}

func (c *Client) GetTransaction(ctx context.Context, signature Signature, opts TransactionOptions) (json.RawMessage, error) {
	if err := signature.Validate(); err != nil {
		return nil, err
	}
	commitment, err := c.selectCommitment(opts.Commitment)
	if err != nil {
		return nil, err
	}
	if commitment == Processed {
		return nil, errors.New("solana getTransaction does not support processed commitment")
	}
	config := map[string]any{"commitment": commitment, "encoding": "json"}
	config["maxSupportedTransactionVersion"] = transactionVersion(opts.MaxSupportedTransactionVersion)
	var result json.RawMessage
	err = c.call(ctx, "getTransaction", []any{signature, config}, &result)
	return result, err
}

func (c *Client) GetSignaturesForAddress(ctx context.Context, address PublicKey, opts SignaturesOptions) ([]SignatureInfo, error) {
	if err := address.Validate(); err != nil {
		return nil, err
	}
	commitment, err := c.selectCommitment(opts.Commitment)
	if err != nil {
		return nil, err
	}
	config := map[string]any{"commitment": commitment}
	if opts.Before != "" {
		if err := opts.Before.Validate(); err != nil {
			return nil, err
		}
		config["before"] = opts.Before
	}
	if opts.Until != "" {
		if err := opts.Until.Validate(); err != nil {
			return nil, err
		}
		config["until"] = opts.Until
	}
	if opts.Limit != 0 {
		if opts.Limit < 1 || opts.Limit > 1000 {
			return nil, errors.New("solana signatures limit must be between 1 and 1000")
		}
		config["limit"] = opts.Limit
	}
	if opts.MinContextSlot != nil {
		config["minContextSlot"] = *opts.MinContextSlot
	}
	var result []SignatureInfo
	err = c.call(ctx, "getSignaturesForAddress", []any{address, config}, &result)
	return result, err
}

func (c *Client) GetAccountInfo(ctx context.Context, address PublicKey, opts AccountOptions) (AccountInfo, error) {
	if err := address.Validate(); err != nil {
		return AccountInfo{}, err
	}
	config, err := c.accountConfig(opts)
	if err != nil {
		return AccountInfo{}, err
	}
	var result AccountInfo
	err = c.call(ctx, "getAccountInfo", []any{address, config}, &result)
	return result, err
}

func (c *Client) GetProgramAccounts(ctx context.Context, program PublicKey, opts ProgramAccountsOptions) ([]ProgramAccount, error) {
	if err := program.Validate(); err != nil {
		return nil, err
	}
	config, err := c.programAccountsConfig(opts)
	if err != nil {
		return nil, err
	}
	var result []ProgramAccount
	err = c.call(ctx, "getProgramAccounts", []any{program, config}, &result)
	return result, err
}

func (c *Client) accountConfig(opts AccountOptions) (map[string]any, error) {
	commitment, err := c.selectCommitment(opts.Commitment)
	if err != nil {
		return nil, err
	}
	encoding := opts.Encoding
	if encoding == "" {
		encoding = Base64
	}
	if err := encoding.validate(); err != nil {
		return nil, err
	}
	config := map[string]any{"commitment": commitment, "encoding": encoding}
	if opts.DataSlice != nil {
		config["dataSlice"] = opts.DataSlice
	}
	if opts.MinContextSlot != nil {
		config["minContextSlot"] = *opts.MinContextSlot
	}
	return config, nil
}

func (c *Client) programAccountsConfig(opts ProgramAccountsOptions) (map[string]any, error) {
	config, err := c.accountConfig(AccountOptions{
		Commitment:     opts.Commitment,
		Encoding:       opts.Encoding,
		DataSlice:      opts.DataSlice,
		MinContextSlot: opts.MinContextSlot,
	})
	if err != nil {
		return nil, err
	}
	if len(opts.Filters) > 0 {
		for _, filter := range opts.Filters {
			if !json.Valid(filter) {
				return nil, errors.New("solana program account filter is invalid JSON")
			}
		}
		config["filters"] = opts.Filters
	}
	return config, nil
}

func transactionVersion(configured *uint8) uint8 {
	if configured == nil {
		return 0
	}
	return *configured
}
