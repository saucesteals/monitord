package solana

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/saucesteals/monitord/catalog/quicknode"
)

type SlotNotification struct {
	Parent Slot `json:"parent"`
	Root   Slot `json:"root"`
	Slot   Slot `json:"slot"`
}

type LogsFilter struct {
	IncludeVotes bool
	Mentions     PublicKey
}

type LogsNotification struct {
	Context Context   `json:"context"`
	Value   LogsValue `json:"value"`
}

type LogsValue struct {
	Signature Signature       `json:"signature"`
	Err       json.RawMessage `json:"err"`
	Logs      []string        `json:"logs"`
}

type TransactionAccountsFilter struct {
	Include  []PublicKey
	Exclude  []PublicKey
	Required []PublicKey
}

type TransactionFilter struct {
	ExcludeVotes  bool
	ExcludeFailed bool
	Signature     Signature
	Accounts      TransactionAccountsFilter
}

type TransactionNotification struct {
	Context Context                      `json:"context"`
	Value   TransactionNotificationValue `json:"value"`
}

type TransactionNotificationValue struct {
	Signature   Signature       `json:"signature"`
	Slot        Slot            `json:"slot"`
	Err         json.RawMessage `json:"err"`
	Transaction json.RawMessage `json:"transaction"`
}

type ProgramNotification struct {
	Context Context        `json:"context"`
	Value   ProgramAccount `json:"value"`
}

func (c *Client) SubscribeSlots(ctx context.Context) (*quicknode.Subscription[SlotNotification], error) {
	return subscribe[SlotNotification](ctx, c, "slotSubscribe", "slotNotification", []any{})
}

func (c *Client) SubscribeLogs(ctx context.Context, filter LogsFilter, commitment Commitment) (*quicknode.Subscription[LogsNotification], error) {
	selected, err := c.selectCommitment(commitment)
	if err != nil {
		return nil, err
	}
	var wireFilter any = "all"
	if filter.Mentions != "" {
		if err := filter.Mentions.Validate(); err != nil {
			return nil, err
		}
		wireFilter = map[string]any{"mentions": []PublicKey{filter.Mentions}}
	} else if filter.IncludeVotes {
		wireFilter = "allWithVotes"
	}
	return subscribe[LogsNotification](ctx, c, "logsSubscribe", "logsNotification", []any{
		wireFilter,
		map[string]any{"commitment": selected},
	})
}

// SubscribeTransactions streams complete QuickNode transaction records after
// applying account, outcome, and vote filters at the provider.
func (c *Client) SubscribeTransactions(ctx context.Context, filter TransactionFilter, commitment Commitment) (*quicknode.Subscription[TransactionNotification], error) {
	selected, err := c.selectCommitment(commitment)
	if err != nil {
		return nil, err
	}
	wireFilter := map[string]any{}
	if filter.ExcludeVotes {
		wireFilter["vote"] = false
	}
	if filter.ExcludeFailed {
		wireFilter["failed"] = false
	}
	if filter.Signature != "" {
		if err := filter.Signature.Validate(); err != nil {
			return nil, err
		}
		wireFilter["signature"] = filter.Signature
	}
	accounts := map[string]any{}
	for key, values := range map[string][]PublicKey{
		"include":  filter.Accounts.Include,
		"exclude":  filter.Accounts.Exclude,
		"required": filter.Accounts.Required,
	} {
		for _, address := range values {
			if err := address.Validate(); err != nil {
				return nil, err
			}
		}
		if len(values) > 0 {
			accounts[key] = values
		}
	}
	if len(accounts) > 0 {
		wireFilter["accounts"] = accounts
	}
	if filter.Signature == "" && len(accounts) == 0 {
		return nil, errors.New("solana transaction subscription requires a signature or account filter")
	}
	return subscribe[TransactionNotification](ctx, c, "transactionSubscribe", "transactionNotification", []any{
		wireFilter,
		map[string]any{
			"commitment":                     selected,
			"encoding":                       "json",
			"transactionDetails":             "full",
			"showRewards":                    false,
			"maxSupportedTransactionVersion": 0,
		},
	})
}

func (c *Client) SubscribeAccount(ctx context.Context, address PublicKey, opts AccountOptions) (*quicknode.Subscription[AccountInfo], error) {
	if err := address.Validate(); err != nil {
		return nil, err
	}
	config, err := c.accountConfig(opts)
	if err != nil {
		return nil, err
	}
	return subscribe[AccountInfo](ctx, c, "accountSubscribe", "accountNotification", []any{address, config})
}

func (c *Client) SubscribeProgram(ctx context.Context, program PublicKey, opts ProgramAccountsOptions) (*quicknode.Subscription[ProgramNotification], error) {
	if err := program.Validate(); err != nil {
		return nil, err
	}
	config, err := c.programAccountsConfig(opts)
	if err != nil {
		return nil, err
	}
	return subscribe[ProgramNotification](ctx, c, "programSubscribe", "programNotification", []any{program, config})
}

func subscribe[T any](ctx context.Context, client *Client, subscribeMethod, notificationMethod string, params []any) (*quicknode.Subscription[T], error) {
	return quicknode.Subscribe[T](ctx, client.provider, quicknode.SubscriptionSpec{
		SubscribeMethod:    subscribeMethod,
		NotificationMethod: notificationMethod,
		Params:             params,
	}, nil)
}
