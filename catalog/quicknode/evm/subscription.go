package evm

import (
	"context"
	"encoding/json"

	"github.com/saucesteals/monitord/catalog/quicknode"
)

type Head struct {
	Number     string `json:"number"`
	Hash       Hash   `json:"hash"`
	ParentHash Hash   `json:"parentHash"`
}

func (c *Client) SubscribeHeads(ctx context.Context) (*quicknode.Subscription[Head], error) {
	return subscribe[Head](ctx, c, "newHeads", nil)
}

func (c *Client) SubscribePending(ctx context.Context) (*quicknode.Subscription[Hash], error) {
	return subscribe[Hash](ctx, c, "newPendingTransactions", nil)
}

func (c *Client) SubscribeLogs(ctx context.Context, filter Logs) (*quicknode.Subscription[Log], error) {
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	arg := map[string]any{}
	if len(filter.Addresses) > 0 {
		arg["address"] = filter.Addresses
	}
	if len(filter.Topics) > 0 {
		arg["topics"] = filter.Topics
	}
	return subscribe(ctx, c, "logs", func(raw json.RawMessage) (Log, error) {
		var wire rpcLog
		if err := json.Unmarshal(raw, &wire); err != nil {
			return Log{}, err
		}
		return decodeRPCLog(wire, c.chainID)
	}, arg)
}

func subscribe[T any](ctx context.Context, client *Client, kind string, decode func(json.RawMessage) (T, error), args ...any) (*quicknode.Subscription[T], error) {
	params := append([]any{kind}, args...)
	return quicknode.Subscribe(ctx, client.provider, quicknode.SubscriptionSpec{
		SubscribeMethod:    "eth_subscribe",
		NotificationMethod: "eth_subscription",
		Params:             params,
	}, decode)
}
