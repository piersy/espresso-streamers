package op

import (
	"sync"

	"github.com/EspressoSystems/espresso-streamers/op/derivation"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// batchStore holds the batches the streamer has validated but not yet handed to its
// consumer, keyed by L2 block number.
//
// One batch per height is enough because only fully validated batches get in: the
// fetch loop defers a HotShot block it cannot decide rather than parking an undecided
// batch here. Anyone can post to the namespace, but a forged batch is dropped on the
// signer check before it reaches the store, so a second batch arriving for a height
// that is already taken means the authorized batcher produced two blocks at one
// height. The first one HotShot ordered wins - deterministic across streamers, since
// blocks are processed in HotShot order - and the later one is logged and ignored.
type batchStore struct {
	// batches maps L2 block number to the batch for that height.
	batches map[uint64]*derivation.EspressoBatch

	mu           sync.RWMutex
	nextBatchPos uint64

	// tipHash is the block hash of the last batch handed to the consumer
	tipHash common.Hash
	// lastPeeked is the batch most recently returned by peek, remembered so advance
	// can promote exactly that batch rather than looking the position up again -
	// advanceOnFinalization may have pruned it in the meantime.
	lastPeeked *derivation.EspressoBatch

	lastFinalizedL2 uint64
	log             log.Logger
}

func newBatchStore(nextBatchPos uint64, tipHash common.Hash, logger log.Logger) *batchStore {
	return &batchStore{
		batches:      make(map[uint64]*derivation.EspressoBatch),
		nextBatchPos: nextBatchPos,
		tipHash:      tipHash,
		log:          logger,
	}
}

// insert records a validated batch at its height. The first batch to claim a height
// keeps it; see the type comment for why a later one is the batcher equivocating.
func (s *batchStore) insert(batch *derivation.EspressoBatch) {
	num := batch.Number()
	parentHash := batch.BatchHeader.ParentHash
	hash := batch.Hash()

	// Every log below is emitted after unlocking, deliberately: this runs once per batch
	// off the HotShot loop, and peek takes the same write lock, so a log write held under
	// it would stall the derivation caller.
	s.mu.Lock()

	// Already finalized, no need to insert
	if num <= s.lastFinalizedL2 {
		s.mu.Unlock()
		return
	}

	if held, taken := s.batches[num]; taken {
		s.mu.Unlock()
		// Hashing outside the lock: it is not free, and nothing here needs the lock.
		if heldHash := held.Hash(); heldHash != hash {
			s.log.Warn(
				"ignoring a second batch for a height already held: the authorized batcher produced more than one block here",
				"batchNr", num,
				"held", heldHash,
				"ignored", hash,
				"parentHash", parentHash,
			)
			return
		}
		s.log.Info(
			"ignoring duplicate batch",
			"batchNr", num,
			"hash", hash,
			"parentHash", parentHash,
		)
		return
	}

	s.batches[num] = batch
	s.mu.Unlock()

	s.log.Info(
		"stored batch",
		"batchNr", num,
		"hash", hash,
		"parentHash", parentHash,
	)
}

// peek returns the batch at the current position if it extends the tracked tip,
// remembering it so advance can promote it without the caller naming it - hence the
// write lock. Everything the store holds has already been validated, so there is no
// verdict to reach here and no network call to make.
func (s *batchStore) peek() *derivation.EspressoBatch {
	// As in insert, logs are emitted after unlocking so they do not hold off the inserts
	// coming from the HotShot loop.
	s.mu.Lock()

	s.lastPeeked = nil

	next, ok := s.batches[s.nextBatchPos]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	// Unreachable by construction, so fail closed rather than handing out a batch we
	// cannot show extends the chain.
	if s.tipHash == (common.Hash{}) {
		blockNr := s.nextBatchPos
		s.mu.Unlock()
		s.log.Error(
			"tip hash unset, refusing to serve a batch",
			"blockNr", blockNr,
		)
		return nil
	}
	// The batch is valid in itself but builds on a block we did not serve, so serving
	// it would break the chain. Only a batcher that forked can produce this.
	if next.BatchHeader.ParentHash != s.tipHash {
		blockNr, tip, parent := s.nextBatchPos, s.tipHash, next.BatchHeader.ParentHash
		s.mu.Unlock()
		s.log.Warn(
			"held batch does not extend the tip",
			"blockNr", blockNr,
			"tip", tip,
			"parentHash", parent,
		)
		return nil
	}

	s.lastPeeked = next
	s.mu.Unlock()
	return next
}

// finalizedL2 returns the highest L2 block number known to be finalized. Batches at
// or below it have already been derived.
func (s *batchStore) finalizedL2() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastFinalizedL2
}

// tip returns the parent hash the next batch must declare, or the zero hash if no
// batch has been consumed yet.
func (s *batchStore) tip() common.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tipHash
}

// advance records that the batch last returned by peek has been consumed: it
// becomes the tip, so the next peek looks for its child. Without a peeked batch it
// is a no-op: advancing only the position would wedge peek permanently (#485).
func (s *batchStore) advance() {
	s.mu.Lock()

	if s.lastPeeked == nil {
		blockNr, tip := s.nextBatchPos, s.tipHash
		s.mu.Unlock()
		s.log.Warn(
			"advance without a peeked batch, refusing to move the position",
			"blockNr", blockNr,
			"tip", tip,
		)
		return
	}
	s.tipHash = s.lastPeeked.BatchHeader.Hash()
	s.lastPeeked = nil
	s.nextBatchPos++
	s.mu.Unlock()
}

// setBatchPosition repositions the store onto the tip the caller knows to be
// canonical, dropping whatever it was tracking.
func (s *batchStore) setBatchPosition(nextBatchPos uint64, tipHash common.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextBatchPos = nextBatchPos
	s.tipHash = tipHash
	s.lastPeeked = nil
}

func (s *batchStore) advanceOnFinalization(finalizedL2 uint64) {
	s.mu.Lock()
	if finalizedL2 <= s.lastFinalizedL2 {
		s.mu.Unlock()
		return
	}

	// Ranging over the heights held, not every number up to finalizedL2: the first call
	// after startup would otherwise iterate the whole chain under the write lock.
	for height := range s.batches {
		if height <= finalizedL2 {
			delete(s.batches, height)
		}
	}
	s.lastFinalizedL2 = finalizedL2
	nextBatchPos, remaining := s.nextBatchPos, len(s.batches)
	s.mu.Unlock()

	s.log.Info(
		"pruned finalized slots",
		"finalizedL2", finalizedL2,
		"nextBatchPos", nextBatchPos,
		"batches", remaining,
	)
}
