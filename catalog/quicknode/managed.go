package quicknode

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/saucesteals/monitord"
	"golang.org/x/crypto/sha3"
)

const (
	defaultPollInterval = 2 * time.Second
	checkpointSource    = "quicknode.confirmed-blocks"
)

var _ monitord.Monitor[WalletState] = Wallet{}
var _ monitord.Monitor[struct{}] = Events[struct{}]{}

type Events[S any] struct {
	Name            string
	Filter          Logs
	ExpectedChainID ChainID
	Confirmations   uint64
	Handle          func(*monitord.Tx[S], Log) error
	WSSURL          string
	HTTPURL         string
}

func (e Events[S]) Info() monitord.Info {
	name := e.Name
	if name == "" {
		name = "quicknode-events"
	}
	return monitord.Info{Name: name, Description: "Confirmed QuickNode Ethereum logs"}
}

func (e Events[S]) Plan() monitord.Plan[S] {
	refs := []monitord.SecretRef{}
	if e.WSSURL == "" {
		refs = append(refs, monitord.RequiredSecret("quicknode", "QUICKNODE_WSS_URL"))
	}
	if e.HTTPURL == "" {
		refs = append(refs, monitord.OptionalSecret("quicknode", "QUICKNODE_HTTP_URL"))
	}
	return monitord.Continuous(e.run, monitord.WithSecrets(refs...))
}

func (e Events[S]) run(ctx context.Context, s *monitord.Session[S]) error {
	if e.Handle == nil {
		return errors.New("quicknode: Events.Handle is required")
	}
	if err := e.Filter.Validate(); err != nil {
		return err
	}
	httpURL := e.HTTPURL
	if httpURL == "" {
		httpURL, _ = s.Secrets().Get("quicknode", "QUICKNODE_HTTP_URL")
	}
	if httpURL == "" {
		wss := e.WSSURL
		if wss == "" {
			wss, _ = s.Secrets().Get("quicknode", "QUICKNODE_WSS_URL")
		}
		var err error
		httpURL, err = HTTPFromWSS(wss)
		if err != nil {
			return err
		}
	}
	c, err := Open(ctx, Config{HTTPURL: httpURL})
	if err != nil {
		return err
	}
	defer c.Close()
	if e.ExpectedChainID != "" && c.ChainID() != e.ExpectedChainID {
		return fmt.Errorf("quicknode: expected chain %s, endpoint is %s", e.ExpectedChainID, c.ChainID())
	}
	confirmations, err := confirmationDepth(c.ChainID(), e.Confirmations)
	if err != nil {
		return err
	}
	if err := s.Progress(ctx); err != nil {
		return err
	}
	// Checkpoints are daemon-owned but not exposed as a Session read API. Start
	// inclusively from genesis; durable event IDs make restarts safe. The daemon
	// can optimize this once checkpoint snapshots are exposed to catalog plans.
	var durable Checkpoint
	found, err := s.Checkpoint(checkpointSource, &durable)
	if err != nil {
		return err
	}
	var next uint64
	if found {
		if durable.ChainID != "" && durable.ChainID != c.ChainID() {
			return fmt.Errorf("quicknode: checkpoint chain %s differs from endpoint %s", durable.ChainID, c.ChainID())
		}
		next = durable.NextBlock
		if next > 0 && durable.CanonicalParent != "" {
			prior, loadErr := c.blockByNumber(ctx, next-1, false)
			if loadErr != nil {
				return loadErr
			}
			if prior.Hash != durable.CanonicalParent {
				if next > confirmations {
					next -= confirmations
				} else {
					next = 0
				}
			}
		}
	}
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()
	for {
		head, err := c.blockNumber(ctx)
		if err != nil {
			return err
		}
		if head >= confirmations {
			target := head - confirmations
			for next <= target {
				if err := e.processBlock(ctx, s, c, next); err != nil {
					return err
				}
				next++
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e Events[S]) processBlock(ctx context.Context, s *monitord.Session[S], c *Client, n uint64) error {
	block, err := c.blockByNumber(ctx, n, false)
	if err != nil {
		return err
	}
	logs, err := c.logs(ctx, e.Filter, n, n)
	if err != nil {
		return err
	}
	for _, log := range logs {
		log.ChainID = c.ChainID()
		log = log.Clone()
		if err := s.Commit(ctx, func(tx *monitord.Tx[S]) error {
			if err := e.Handle(tx, log); err != nil {
				return err
			}
			if err := tx.Checkpoint(checkpointSource, Checkpoint{ChainID: c.ChainID(), NextBlock: n, CanonicalParent: block.Hash}); err != nil {
				return err
			}
			tx.Progress()
			return nil
		}); err != nil {
			return err
		}
	}
	return s.Commit(ctx, func(tx *monitord.Tx[S]) error {
		if err := tx.Checkpoint(checkpointSource, Checkpoint{ChainID: c.ChainID(), NextBlock: n + 1, CanonicalParent: block.Hash}); err != nil {
			return err
		}
		tx.Progress()
		return nil
	})
}

func confirmationDepth(chain ChainID, configured uint64) (uint64, error) {
	if configured > 0 {
		return configured, nil
	}
	if chain == "0x1" {
		return 12, nil
	}
	return 0, fmt.Errorf("quicknode: confirmations are required for chain %s", chain)
}

func HTTPFromWSS(raw string) (string, error) {
	u, err := endpoint(raw, "ws", "wss")
	if err != nil {
		return "", fmt.Errorf("quicknode: derive HTTP endpoint: %w", err)
	}
	if u.Scheme == "wss" {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	return u.String(), nil
}

type rpcBlock struct {
	Number       string           `json:"number"`
	Hash         Hash             `json:"hash"`
	ParentHash   Hash             `json:"parentHash"`
	Transactions []rpcTransaction `json:"transactions"`
}
type rpcTransaction struct {
	Hash             Hash     `json:"hash"`
	From             Address  `json:"from"`
	To               *Address `json:"to"`
	Value            string   `json:"value"`
	TransactionIndex string   `json:"transactionIndex"`
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
	var b rpcBlock
	err := c.call(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", n), full}, &b)
	if err == nil {
		number, parseErr := parseUintQuantity(b.Number)
		if parseErr != nil || number != n {
			return rpcBlock{}, errors.New("quicknode: invalid block response")
		}
		if _, parseErr = ParseHash(string(b.Hash)); parseErr != nil {
			return rpcBlock{}, parseErr
		}
		if _, parseErr = ParseHash(string(b.ParentHash)); parseErr != nil {
			return rpcBlock{}, parseErr
		}
	}
	return b, err
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

type Wallet struct {
	Name            string
	WSSURL          string
	HTTPURL         string
	ExpectedChainID ChainID
	Address         Address
	Events          TransferKinds
	Confirmations   uint64
	Map             func(Transfer) monitord.Event
}
type WalletState struct {
	Checkpoint Checkpoint `json:"checkpoint"`
}
type walletJournal struct {
	Blocks []walletJournalBlock `json:"blocks"`
}
type walletJournalBlock struct {
	Number    uint64     `json:"number"`
	Hash      Hash       `json:"hash"`
	Transfers []Transfer `json:"transfers"`
}

const walletJournalSource = "quicknode.wallet-canonical-journal"

func (w Wallet) Info() monitord.Info {
	name := w.Name
	if name == "" {
		name = "quicknode-wallet"
	}
	return monitord.Info{Name: name, Description: "Confirmed native and token transfers for an EVM wallet"}
}
func (w Wallet) Plan() monitord.Plan[WalletState] {
	return monitord.Continuous(w.run, monitord.WithSecrets(w.secretRefs()...))
}
func (w Wallet) secretRefs() []monitord.SecretRef {
	r := []monitord.SecretRef{}
	if w.WSSURL == "" {
		r = append(r, monitord.RequiredSecret("quicknode", "QUICKNODE_WSS_URL"))
	}
	if w.HTTPURL == "" {
		r = append(r, monitord.OptionalSecret("quicknode", "QUICKNODE_HTTP_URL"))
	}
	if w.Address == "" {
		r = append(r, monitord.RequiredSecret("quicknode", "WALLET_ADDRESS"))
	}
	return r
}

func (w Wallet) run(ctx context.Context, s *monitord.Session[WalletState]) error {
	address := w.Address
	if address == "" {
		v, err := s.Secrets().Require("quicknode", "WALLET_ADDRESS")
		if err != nil {
			return err
		}
		address, err = ParseAddress(v)
		if err != nil {
			return err
		}
	}
	if _, err := ParseAddress(string(address)); err != nil {
		return err
	}
	kinds := w.Events
	if kinds == 0 {
		kinds = AllTransfers
	}
	if kinds&^AllTransfers != 0 {
		return errors.New("quicknode: invalid wallet transfer kinds")
	}
	httpURL := w.HTTPURL
	if httpURL == "" {
		httpURL, _ = s.Secrets().Get("quicknode", "QUICKNODE_HTTP_URL")
	}
	if httpURL == "" {
		ws := w.WSSURL
		if ws == "" {
			ws, _ = s.Secrets().Get("quicknode", "QUICKNODE_WSS_URL")
		}
		var err error
		httpURL, err = HTTPFromWSS(ws)
		if err != nil {
			return err
		}
	}
	c, err := Open(ctx, Config{HTTPURL: httpURL})
	if err != nil {
		return err
	}
	defer c.Close()
	if w.ExpectedChainID != "" && w.ExpectedChainID != c.ChainID() {
		return fmt.Errorf("quicknode: expected chain %s, endpoint is %s", w.ExpectedChainID, c.ChainID())
	}
	depth, err := confirmationDepth(c.ChainID(), w.Confirmations)
	if err != nil {
		return err
	}
	if err = s.Progress(ctx); err != nil {
		return err
	}
	next := s.State().Checkpoint.NextBlock
	var journal walletJournal
	_, err = s.Checkpoint(walletJournalSource, &journal)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()
	for {
		head, err := c.blockNumber(ctx)
		if err != nil {
			return err
		}
		if head >= depth {
			target := head - depth
			if next > 0 && s.State().Checkpoint.CanonicalParent != "" {
				prior, loadErr := c.blockByNumber(ctx, next-1, false)
				if loadErr != nil {
					return loadErr
				}
				if prior.Hash != s.State().Checkpoint.CanonicalParent {
					rewind, orphaned, reconcileErr := reconcileJournal(ctx, c, journal, next)
					if reconcileErr != nil {
						return reconcileErr
					}
					journal.Blocks = journal.Blocks[:rewindJournalIndex(journal, rewind)]
					if err = s.Commit(ctx, func(tx *monitord.Tx[WalletState]) error {
						for _, tr := range orphaned {
							ev := w.correctionEvent(tr)
							if emitErr := tx.Emit(ev); emitErr != nil {
								return emitErr
							}
						}
						tx.State.Checkpoint = Checkpoint{ChainID: c.ChainID(), NextBlock: rewind}
						if cpErr := tx.Checkpoint(checkpointSource, tx.State.Checkpoint); cpErr != nil {
							return cpErr
						}
						return tx.Checkpoint(walletJournalSource, journal)
					}); err != nil {
						return err
					}
					next = rewind
				}
			}
			for next <= target {
				block, err := c.blockByNumber(ctx, next, true)
				if err != nil {
					return err
				}
				transfers, err := w.blockTransfers(ctx, c, address, kinds, block)
				if err != nil {
					return err
				}
				nextJournal := walletJournal{Blocks: append([]walletJournalBlock(nil), journal.Blocks...)}
				nextJournal.Blocks = append(nextJournal.Blocks, walletJournalBlock{Number: next, Hash: block.Hash, Transfers: append([]Transfer(nil), transfers...)})
				if len(nextJournal.Blocks) > 256 {
					nextJournal.Blocks = append([]walletJournalBlock(nil), nextJournal.Blocks[len(nextJournal.Blocks)-256:]...)
				}
				if err = s.Commit(ctx, func(tx *monitord.Tx[WalletState]) error {
					for _, tr := range transfers {
						ev := w.mapEventFor(tr, address)
						if err := tx.Emit(ev); err != nil {
							return err
						}
					}
					tx.State.Checkpoint = Checkpoint{ChainID: c.ChainID(), NextBlock: next + 1, CanonicalParent: block.Hash}
					if err := tx.Checkpoint(checkpointSource, tx.State.Checkpoint); err != nil {
						return err
					}
					if err := tx.Checkpoint(walletJournalSource, nextJournal); err != nil {
						return err
					}
					tx.Progress()
					return nil
				}); err != nil {
					return err
				}
				journal = nextJournal
				next++
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func rewindJournalIndex(j walletJournal, block uint64) int {
	for i, b := range j.Blocks {
		if b.Number >= block {
			return i
		}
	}
	return len(j.Blocks)
}
func reconcileJournal(ctx context.Context, c *Client, j walletJournal, next uint64) (uint64, []Transfer, error) {
	for i := len(j.Blocks) - 1; i >= 0; i-- {
		canonical, err := c.blockByNumber(ctx, j.Blocks[i].Number, false)
		if err != nil {
			return 0, nil, err
		}
		if canonical.Hash == j.Blocks[i].Hash {
			orphaned := []Transfer{}
			for _, b := range j.Blocks[i+1:] {
				orphaned = append(orphaned, b.Transfers...)
			}
			return j.Blocks[i].Number + 1, orphaned, nil
		}
	}
	orphaned := []Transfer{}
	for _, b := range j.Blocks {
		orphaned = append(orphaned, b.Transfers...)
	}
	rewind := uint64(0)
	if len(j.Blocks) > 0 {
		rewind = j.Blocks[0].Number
	}
	return rewind, orphaned, nil
}
func (w Wallet) correctionEvent(t Transfer) monitord.Event {
	original := w.mapEvent(t)
	return monitord.Event{ID: "correction:" + t.ID(), Title: "Chain reorganization correction", Message: "A previously reported wallet transfer is no longer canonical", Summary: original.Title + ": " + original.Message, Details: "original_event_id=" + t.ID()}
}

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
	return monitord.Event{ID: t.ID(), Title: "Wallet transfer", Message: fmt.Sprintf("%s %s units", direction, t.Amount), Summary: fmt.Sprintf("%s -> %s", t.From, t.To), Details: fmt.Sprintf("chain=%s tx=%s", t.ChainID, t.TxHash)}
}

var _ = json.RawMessage{}
