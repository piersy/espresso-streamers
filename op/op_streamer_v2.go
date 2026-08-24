package op

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	espressoCommon "github.com/EspressoSystems/espresso-network/sdks/go/types"
	"github.com/EspressoSystems/espresso-streamers/op/bindings"
	"github.com/EspressoSystems/espresso-streamers/op/derivation"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	lru "github.com/hashicorp/golang-lru/v2"
)

const pollRPCTimeout = 10 * time.Second

// defaultFinalityInterval paces the finality loop.
const defaultFinalityInterval = 10 * time.Second

type Streamer struct {
	espressoClient           EspressoClient
	batchAuthenticatorCaller *bindings.BatchAuthenticatorCaller
	rollupL1Client           L1Client
	namespace                uint64

	store *batchStore

	// Next HotShot height to read from. Owned by the HotShot loop, which is the only
	// goroutine that touches it, so it needs no lock.
	hotShotPos uint64

	// pendingL1 is the L1 height our finality view must reach before another read can
	// make progress, set when a pass is cut short waiting on finality and zero
	// otherwise. It saves re-reading a range only to defer it again. Owned by the
	// HotShot loop, like hotShotPos.
	pendingL1 uint64

	logger log.Logger

	finalizedL1 uint64

	// Cache for finalized L1 block hashes, keyed by L1 origin block number.
	finalizedL1StateCache *lru.Cache[uint64, l1State]
	// Authorized batcher keyed by the HotShot header's finalized L1 block.
	batcherAtL1FinalizedCache *lru.Cache[uint64, common.Address]
	pollerFunc                func(context.Context) (*eth.SyncStatus, error)

	mu sync.RWMutex

	// How long the HotShot loop waits before retrying after a failed fetch. With a
	// backlog available the loop runs hot.
	retryTime time.Duration

	// How long the HotShot loop waits before polling again once it has caught up with
	// the chain.
	idlePollInterval time.Duration

	// How often the finality loop refreshes its view of L1/L2 finality.
	finalityInterval time.Duration

	// Start/Stop bookkeeping.
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// ErrAlreadyStarted is returned by Start when the poll loop is already running.
var ErrAlreadyStarted = errors.New("streamer already started")

// errAwaitL1Finality reports that a batch cannot be judged until our view of L1
// finality reaches height. It is a control signal rather than a failure: the reader
// stops at the HotShot block carrying the batch and comes back to it, because
// dropping the batch would lose it for good and storing it unjudged is what this
// design sets out to avoid.
type errAwaitL1Finality struct{ height uint64 }

func (e errAwaitL1Finality) Error() string {
	return fmt.Sprintf("awaiting L1 finality at block %d", e.height)
}

// NewStreamer builds a streamer anchored at originBlock, which seeds the tip it tracks.
// The first batch it serves is that block's child.
//
// The caller supplies the block rather than a height for the streamer to resolve. Only
// the caller knows which block it actually processed before shutting down; looking the
// height up on an L2 client could return a different block for the same height after a
// reorg, and the streamer would then extend a chain nobody derived.
func NewStreamer(
	ctx context.Context,
	espressoClient EspressoClient,
	rollupL1Client L1Client,
	batchAuthenticatorAddress common.Address,
	namespace uint64,
	pollerFunc func(context.Context) (*eth.SyncStatus, error),
	retryTime time.Duration,
	idlePollInterval time.Duration,
	logger log.Logger,
	originHotShotPos uint64,
	originBlock eth.BlockID,
) (*Streamer, error) {
	if batchAuthenticatorAddress == (common.Address{}) {
		return nil, fmt.Errorf("BatchAuthenticator address must be set for Espresso streamer")
	}
	if pollerFunc == nil {
		return nil, fmt.Errorf("pollerFunc must be set: the poll loop needs a sync status source")
	}
	// The tip is what every batch is checked against, so an unset one would reject
	// every batch the store is handed. Height 0 is legitimate, a zero hash is not.
	if originBlock.Hash == (common.Hash{}) {
		return nil, fmt.Errorf(
			"originBlock must carry the hash of the block to resume from, got the zero hash at height %d",
			originBlock.Number)
	}
	if retryTime <= 0 {
		return nil, fmt.Errorf("retryTime must be positive, got %s", retryTime)
	}
	if idlePollInterval <= 0 {
		return nil, fmt.Errorf("idlePollInterval must be positive, got %s", idlePollInterval)
	}

	finalizedL1StateCache, _ := lru.New[uint64, l1State](1000)
	batcherAtL1FinalizedCache, _ := lru.New[uint64, common.Address](1000)
	batchAuthenticatorCaller, err := bindings.NewBatchAuthenticatorCaller(batchAuthenticatorAddress, rollupL1Client)
	if err != nil {
		return nil, fmt.Errorf("failed to bind BatchAuthenticator at %s: %w", batchAuthenticatorAddress, err)
	}
	return &Streamer{
		espressoClient:            espressoClient,
		namespace:                 namespace,
		pollerFunc:                pollerFunc,
		logger:                    logger,
		retryTime:                 retryTime,
		idlePollInterval:          idlePollInterval,
		finalityInterval:          defaultFinalityInterval,
		store:                     newBatchStore(originBlock, logger),
		hotShotPos:                originHotShotPos,
		finalizedL1StateCache:     finalizedL1StateCache,
		batcherAtL1FinalizedCache: batcherAtL1FinalizedCache,
		rollupL1Client:            rollupL1Client,
		batchAuthenticatorCaller:  batchAuthenticatorCaller,
	}, nil
}

// Start launches the two background loops - one tracking finality and one pulling from
// Espresso it returns ErrAlreadyStarted if they are already running.
func (s *Streamer) Start(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.cancel != nil {
		return ErrAlreadyStarted
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Initialize finality. pollForFinality scopes its own per-phase timeouts.
	s.pollForFinality(ctx)
	primeCtx, cancelPrime := context.WithTimeout(ctx, pollRPCTimeout)
	s.pollForFinality(primeCtx)
	cancelPrime()

	s.logger.Info("espresso streamer started",
		"hotShotPos", s.hotShotPos, "retryTime", s.retryTime, "finalityInterval", s.finalityInterval)

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.pollFinality(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.pollHotShot(ctx)
	}()
	return nil
}

// Stop cancels both loops and blocks until they have returned.
func (s *Streamer) Stop() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.cancel == nil {
		return
	}

	s.cancel()
	s.wg.Wait()
	s.cancel = nil

	s.logger.Info("espresso streamer stopped")
}

// Next returns the batch extending the tip the streamer is tracking and moves the tip
// onto it, or nil if there is none. It errors when the batch held at that position does
// not extend the tip, since serving it would break the chain.
//
// The tip only ever moves forward here, and only for a batch actually handed over, so
// no block can be skipped. A consumer that could not use the batch it received calls
// RewindTip to be served it again - taking a batch does not drop it.
func (s *Streamer) Next() (*derivation.EspressoBatch, error) {
	return s.store.next()
}

// RewindTip rewinds the streamer's tip to the given point, so the next batch it serves is that
// block's child. This allows re-reading batches from the tip onwards, which is required in the
// case of a batcher channel reset. It fails if the given point is ahead of the current tip, since
// the tip otherwise advances only as the consumer reads.
//
// There is a potential edge case where the requested tip could already have been pruned, which
// would happen if the streamer's finality view advanced past the batcher's safe head view
// (batcher channel resets rewind to the safe head). As long as the streamer and the batcher take
// finality from the same source this should be mitigated, since the finalized head trails the
// safe head by ~12 minutes and both poll far more often than that.
func (s *Streamer) RewindTip(tip eth.BlockID) error {
	return s.store.rewindTip(tip)
}

// pollFinality keeps the streamer's view of finality current, on an interval. It is
// deliberately not hot: finality only advances at L1 block time, and each pass costs a
// sync-status call plus an L1 header read. Running it apart from the HotShot loop
// also keeps a slow fetch from delaying finality, and vice versa.
func (s *Streamer) pollFinality(ctx context.Context) {
	defer s.logger.Info("finality poll loop returning")

	ticker := time.NewTicker(s.finalityInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.finalityTick(ctx)
	}
}

// finalityTick refreshes the finality view and prunes the store. pollForFinality
// validates the reading against the local chain before returning it, so a non-zero
// height here is safe to act on; advanceOnFinalization carries its own guards.
func (s *Streamer) finalityTick(ctx context.Context) {
	finalizedL2 := s.pollForFinality(ctx)
	if finalizedL2 == 0 {
		return
	}
	s.store.advanceOnFinalization(finalizedL2)
}

// pollHotShot runs hot while there is a backlog: it keeps pulling from HotShot as fast
// as the query service will serve, so a backlog is consumed at the rate the endpoints
// allow rather than one batch of blocks per tick. It pauses for idlePollInterval when a
// pass made no progress, and for retryTime when one failed.
func (s *Streamer) pollHotShot(ctx context.Context) {
	defer s.logger.Info("hotshot poll loop returning")

	for ctx.Err() == nil {
		fetchCtx, cancelFetch := context.WithTimeout(ctx, pollRPCTimeout)
		idle, err := s.fetchEspressoTransactions(fetchCtx)
		cancelFetch()

		if err == nil {
			if !idle {
				continue
			}
			// Nothing moved, either because we are caught up with HotShot or because
			// we are waiting on L1 finality. Pace the polls rather than spin.
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.idlePollInterval):
			}
			continue
		}
		select {
		case <-ctx.Done():
			s.logger.Info("fetch interrupted by shutdown", "err", err)
			return
		default:
		}
		s.logger.Warn("failed to fetch espresso transactions", "err", err, "retryIn", s.retryTime)

		// Back off, so a persistently failing endpoint is retried rather than hammered.
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.retryTime):
		}
	}
}

// pollForFinality refreshes the streamer's view of L1 finality, returning the finalized
// L2 height it saw so the caller can decide whether the store needs pruning. It returns
// 0 when no usable reading was available.
func (s *Streamer) pollForFinality(ctx context.Context) uint64 {
	// Each phase gets its own timeout so a slow phase cannot starve the ones after
	// it (the L2 validation below must not inherit an exhausted budget).
	statusCtx, cancelStatus := context.WithTimeout(ctx, pollRPCTimeout)
	defer cancelStatus()
	syncStatus, err := s.pollerFunc(statusCtx)
	if err != nil {
		s.logger.Warn("failed to fetch sync status", "err", err)
		return 0
	}
	if syncStatus == nil {
		s.logger.Warn("sync status is nil")
		return 0
	}
	// L1 finality is read from the L1 client rather than the sync status, because it is
	// the same client the origin lookups in checkBatch go to. Taking a higher number
	// from anywhere else would let those lookups run against blocks this node has not
	// finalized, or does not have at all.
	header, err := s.rollupL1Client.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		s.logger.Warn("failed to fetch the finalized L1 header, keeping the current view", "err", err)
		return 0
	}
	if header == nil || header.Number == nil {
		s.logger.Warn("finalized L1 header is empty, keeping the current view")
		return 0
	}

	s.mu.Lock()
	// L1 finality is monotonic, so a lower number means the sync source regressed
	if header.Number.Uint64() < s.finalizedL1 {
		s.mu.Unlock()
		s.logger.Warn("ignoring regressed finalized L1 block",
			"current", s.finalizedL1, "reported", header.Number.Uint64())
		return 0
	}
	s.finalizedL1 = header.Number.Uint64()
	s.mu.Unlock()

	// Nothing is finalized yet, so there is no L1 origin to pin a HotShot height to.
	if syncStatus.FinalizedL2 == (eth.L2BlockRef{}) {
		return 0
	}

	return syncStatus.FinalizedL2.Number
}

// fetchEspressoTransactions pulls the next range of HotShot blocks and feeds the store.
//
// It processes a HotShot block only once our own L1 finality view has caught up with
// everything needed to judge that block: the L1 height its header names as finalized,
// which anchors the batcher lookup, and the L1 origin each of its batches declares.
// Where finality falls short the range is truncated and hotShotPos is left short of
// it, so the blocks are read again later. Deferring rather than storing an undecided
// batch is what keeps unvalidated batches out of the store, and deferring rather than
// dropping is what stops a batch being lost, since hotShotPos never rewinds.
//
// Re-reading a block is harmless: the store keeps the first batch at each height, so
// re-processing one it already holds is a no-op.
//
// It reports idle=true when the pass made no progress, either because there was
// nothing new to read or because finality has not moved, so the caller can pace its
// polling instead of spinning (#39).
func (s *Streamer) fetchEspressoTransactions(ctx context.Context) (idle bool, err error) {
	finalizedL1 := s.currentFinalizedL1()

	// Nothing can be judged without a finality view, so do not read anything yet.
	// Start primes one before the loops run, so this only holds if that failed.
	if finalizedL1 == 0 {
		s.logger.Warn("no finalized L1 view yet, not reading from HotShot")
		return true, nil
	}

	// A previous pass stopped waiting on finality and finality has not moved since,
	// so re-reading the range would only reach the same point again.
	if s.pendingL1 > finalizedL1 {
		return true, nil
	}

	finalizedBlockHeight, err := s.espressoClient.FetchLatestBlockHeight(ctx)
	if err != nil {
		return false, err
	}
	// The exclusive end of the fetch range is this height plus one, which would wrap.
	if finalizedBlockHeight == math.MaxUint64 {
		return false, fmt.Errorf("espresso block height overflows uint64")
	}
	if s.hotShotPos >= finalizedBlockHeight {
		return true, nil
	}

	end := s.hotShotPos + HOTSHOT_BLOCK_FETCH_LIMIT

	// Don't go past whats finalized
	if end > finalizedBlockHeight {
		end = finalizedBlockHeight
	}

	blocks, err := s.espressoClient.FetchNamespaceTransactionsInRange(ctx, s.hotShotPos, end, s.namespace)
	if err != nil {
		return false, err
	}

	s.logger.Info("fetched HotShot range", "start", s.hotShotPos, "end", end, "blocks", len(blocks))

	// hotShotPos advances to end below and never rewinds, so a short response would
	// skip the blocks it left out for good.
	if uint64(len(blocks)) != end-s.hotShotPos {
		return false, fmt.Errorf("hotshot range [%d, %d): got %d blocks, want %d",
			s.hotShotPos, end, len(blocks), end-s.hotShotPos)
	}

	// Fetch the headers for the same range so each batch can be authorized against
	// the finalized L1 block of the HotShot block that carried it (see checkBatch).
	headers, err := s.espressoClient.FetchHeadersByRange(ctx, s.hotShotPos, end)
	if err != nil {
		return false, fmt.Errorf("failed to fetch hotshot headers for range [%d, %d): %w", s.hotShotPos, end, err)
	}

	// Batches are positionally associated with headers, so bail rather than risk
	// authorizing a batch against another block's anchor.
	if len(headers) != len(blocks) {
		return false, fmt.Errorf("hotshot header/transaction count mismatch for range [%d, %d): %d headers vs %d blocks",
			s.hotShotPos, end, len(headers), len(blocks))
	}
	// Validate the whole range before processing any of it.
	for i := range headers {
		if got, want := headers[i].Header.GetBlockHeight(), s.hotShotPos+uint64(i); got != want {
			return false, fmt.Errorf("hotshot headers not contiguous/ordered for range [%d, %d): header index %d has height %d, expected %d",
				s.hotShotPos, end, i, got, want)
		}
		// Batches are authorized against this L1 finalized header, so one without it is
		// unusable. It is nil only if Espresso started before the L1 finalized any block
		// (impossible on a live chain)
		// https://github.com/EspressoSystems/espresso-network/blob/main/crates/espresso/types/src/v0/v0_1/l1.rs#L64-L72
		if headers[i].Header.GetL1Finalized() == nil {
			return false, fmt.Errorf("hotshot header at height %d reports no finalized L1 block",
				s.hotShotPos+uint64(i))
		}
	}

	// Feed the blocks, stopping at the first one we cannot judge. The cursor keeps only
	// what was fully processed, so whatever is left is read again once finality moves.
	start := s.hotShotPos
	processed, awaitL1 := 0, uint64(0)
	var processErr error
processing:
	for ; processed < len(blocks); processed++ {
		hotShotHeight := start + uint64(processed)
		// Non-nil, validated above.
		blockL1Finalized := headers[processed].Header.GetL1Finalized().Number

		// The batcher authorized for this block is resolved at the L1 height its header
		// names as finalized, so until our own view has finalized that height the answer
		// could come out of a chain segment that still changes - which is how a
		// rotated-out key would slip through. This is a property of the block rather
		// than of any batch in it, and the header is written by consensus, so no one
		// posting to the namespace can use it to hold the reader up.
		if blockL1Finalized > finalizedL1 {
			awaitL1 = blockL1Finalized
			s.logger.Info("deferring HotShot block whose L1 anchor we have not finalized",
				"hotShotHeight", hotShotHeight,
				"headerL1Finalized", blockL1Finalized,
				"ourL1Finalized", finalizedL1)
			break processing
		}

		for j := range blocks[processed].Transactions {
			await, err := s.process(ctx, hotShotHeight, blockL1Finalized, &blocks[processed].Transactions[j])
			if err != nil {
				processErr = err
				break processing
			}
			if await > 0 {
				awaitL1 = await
				break processing
			}
		}
	}

	// Committing the progress made before the stop, so a failure part-way through a
	// range is not repeated from the start.
	s.hotShotPos = start + uint64(processed)

	// Reading again before finality reaches awaitL1 could only stop at the same place,
	// so record it and skip the work until then. A lookup failure is not about finality,
	// so it clears the wait and is left to the caller's retry backoff.
	s.pendingL1 = awaitL1
	if processErr != nil {
		return false, processErr
	}

	return s.hotShotPos == start, nil
}

// process judges one Espresso transaction and stores the batch it carries if it is
// valid.
//
// awaitL1 is non-zero when the batch cannot be judged until our view of L1 finality
// reaches that height, which tells the caller to stop reading here and come back. A
// returned error is a failed L1 lookup, which is transient and worth retrying.
func (s *Streamer) process(ctx context.Context, hotShotHeight uint64, l1Finalized uint64, txn *espressoCommon.Transaction) (awaitL1 uint64, err error) {
	batch, err := derivation.UnmarshalEspressoTransaction(txn.Payload, l1Finalized)
	if err != nil {
		// Anyone can post to the namespace, so undecodable payloads are ordinary traffic.
		s.logger.Warn("failed to unmarshal batch", "hotShotHeight", hotShotHeight, "err", err)
		return 0, nil
	}

	validity, err := s.checkBatch(ctx, batch)

	var await errAwaitL1Finality
	if errors.As(err, &await) {
		s.logger.Info("deferring HotShot block, a batch in it declares an L1 origin we have not finalized",
			"hotShotHeight", hotShotHeight, "batchNr", batch.Number(),
			"hash", batch.Hash(), "awaitL1", await.height)
		return await.height, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to check batch %d at hotshot height %d: %w", batch.Number(), hotShotHeight, err)
	}

	switch validity {
	case BatchAccept:
		s.store.insert(batch)
	case BatchDrop:
	}
	return 0, nil
}

// checkBatch decides whether a batch is valid. Its signer must be the batcher
// authorized at the batch's L1Finalized - the finalized L1 block reported by the
// HotShot header that carried it - and its declared L1 origin must match a real L1
// block.
//
// The caller must already have established that our own view has finalized
// batch.L1Finalized, since that is what makes the batcher lookup trustworthy.
//
// It returns errAwaitL1Finality when the batch is from the authorized batcher but
// declares an L1 origin we have not finalized, so its hash cannot be compared yet.
// That is not a failure: the caller leaves the batch where it is and reads it again
// once finality has moved. Any other error is a failed L1 lookup.
//
// Since espresso can confirm batches out of order, the parent hash linkage cannot be
// checked here, instead it is checked when each batch is consumed.
func (s *Streamer) checkBatch(ctx context.Context, batch *derivation.EspressoBatch) (BatchValidity, error) {
	l1Finalized := batch.L1Finalized

	// Look up the batcher authorized at l1Finalized which is read from Espresso Header
	authorizedBatcher, ok := s.batcherAtL1FinalizedCache.Get(l1Finalized)
	if !ok {
		batcher, err := s.batchAuthenticatorCaller.EspressoBatcherAtBlock(
			&bind.CallOpts{Context: ctx},
			l1Finalized,
		)
		if err != nil {
			return BatchDrop, fmt.Errorf("failed to fetch the espresso batcher at L1 block %d: %w", l1Finalized, err)
		}
		authorizedBatcher = batcher
		s.batcherAtL1FinalizedCache.Add(l1Finalized, batcher)
	}

	if authorizedBatcher == (common.Address{}) || batch.SignerAddress != authorizedBatcher {
		s.logger.Info(DroppingBatchLogPrefix+" with invalid espresso batcher",
			"batch", batch.Hash(), "signer", batch.SignerAddress,
			"l1Finalized", l1Finalized, "authorizedBatcher", authorizedBatcher)
		return BatchDrop, nil
	}

	// Signer is authorized. The declared L1 origin must be finalized before we can
	// verify its hash. This stays after the signer check deliberately: the origin is
	// declared by the batch and anyone can post to the namespace, so checking it first
	// would let a stranger stall the streamer indefinitely by naming a far-future
	// origin. Only the authorized batcher can make us wait here.
	origin := batch.L1Origin()
	finalizedL1 := s.currentFinalizedL1()
	if origin.Number > finalizedL1 {
		return BatchDrop, errAwaitL1Finality{height: origin.Number}
	}

	// Validate that the batch's declared L1 origin references a real L1 block.
	state, err := s.l1StateAt(ctx, origin.Number)
	if err != nil {
		return BatchDrop, fmt.Errorf("failed to fetch L1 origin state at %d: %w", origin.Number, err)
	}
	if state.hash != origin.Hash {
		s.logger.Warn(DroppingBatchLogPrefix + " with invalid L1 origin hash")
		return BatchDrop, nil
	}
	return BatchAccept, nil
}

// currentFinalizedL1 returns the streamer's view of L1 finality.
func (s *Streamer) currentFinalizedL1() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.finalizedL1
}

// l1StateAt returns the L1 block hash at the given L1 block number, fetching
// from L1 and caching the result on a cache miss.
func (s *Streamer) l1StateAt(ctx context.Context, number uint64) (l1State, error) {
	if state, ok := s.finalizedL1StateCache.Get(number); ok {
		return state, nil
	}

	hash, err := s.rollupL1Client.HeaderHashByNumber(ctx, new(big.Int).SetUint64(number))
	if err != nil {
		return l1State{}, fmt.Errorf("failed to fetch L1 header: %w", err)
	}

	state := l1State{hash: hash}
	s.finalizedL1StateCache.Add(number, state)
	return state, nil
}
