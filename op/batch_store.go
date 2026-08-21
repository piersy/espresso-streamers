package op

import (
	"fmt"
	"sync"

	"github.com/EspressoSystems/espresso-streamers/op/derivation"
	"github.com/ethereum-optimism/optimism/op-service/eth"
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

	mu sync.RWMutex

	// tip is the L2 block the next batch must extend: the last one handed to the
	// consumer, or the block the store was anchored to if none has been. Number and
	// hash are held together so they cannot drift - the batch to serve next is always
	// at tip.Number+1, and its parent must always be tip.Hash.
	tip eth.BlockID

	lastFinalizedL2 uint64
	log             log.Logger
}

func newBatchStore(tip eth.BlockID, logger log.Logger) *batchStore {
	return &batchStore{
		batches: make(map[uint64]*derivation.EspressoBatch),
		tip:     tip,
		log:     logger,
	}
}

// nextPos is the height of the batch the store serves next. Callers must hold the
// lock.
func (s *batchStore) nextPos() uint64 {
	return s.tip.Number + 1
}

// Returns the next batch ensuring that it's parent hash links to the stored parent block, otherwise
// an error is returned. In the case that no next batch is present a nil batch is returned.
//
// It is required that the mutex be held before calling this function.
func (s *batchStore) nextBatch() (*derivation.EspressoBatch, error) {
	n := s.batches[s.nextPos()]
	if n == nil {
		return nil, nil
	}
	if n.BatchHeader.ParentHash != s.tip.Hash {
		return nil, fmt.Errorf(
			"next batch does not extend the tip, blockNum: %d, tipHash: %v, parentHash: %v",
			s.nextPos(),
			s.tip.Hash,
			n.BatchHeader.ParentHash,
		)
	}
	return n, nil
}

// insert records a validated batch at its height. The first batch to claim a height
// keeps it; see the type comment for why a later one is the batcher equivocating.
func (s *batchStore) insert(batch *derivation.EspressoBatch) {
	num := batch.Number()
	parentHash := batch.BatchHeader.ParentHash
	hash := batch.Hash()

	// Every log below is emitted after unlocking, deliberately: this runs once per batch
	// off the HotShot loop, and next contends for the same lock, so a log write held
	// under it would stall the derivation caller.
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

// next returns the batch at the current position and moves the tip onto it, so the
// following call looks for its child. It returns nil when the store holds nothing
// there, and an error when what it holds does not extend the tip.
//
// Handing the batch over and moving the tip are one step, so the position cannot move
// without the consumer receiving the batch and a block cannot be skipped. A consumer
// that could not use what it received winds the tip back with rewindTip and is served
// the same batch again: taking a batch does not drop it from the store, and pruning
// never deletes above the tip.
//
// Everything the store holds has already been validated, so there is no verdict to
// reach here and no network call to make.
func (s *batchStore) next() (*derivation.EspressoBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch, err := s.nextBatch()
	if err != nil || batch == nil {
		return nil, err
	}
	s.tip = eth.BlockID{Hash: batch.BatchHeader.Hash(), Number: batch.Number()}
	return batch, nil
}

// finalizedL2 returns the highest L2 block number known to be finalized. Batches at
// or below it have already been derived.
func (s *batchStore) finalizedL2() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastFinalizedL2
}

// tipRef returns the block the next batch must extend. Its hash is the zero hash if
// the store has no anchor.
func (s *batchStore) tipRef() eth.BlockID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip
}

// rewindTip moves the tip back to an earlier block, so the consumer can re-read the
// batches from there onwards. Rewinding only: the tip otherwise moves solely when the
// consumer reads a batch, so moving it forward here would skip batches the consumer
// never saw.
func (s *batchStore) rewindTip(tip eth.BlockID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tip.Number > s.tip.Number {
		return fmt.Errorf("rewindTip requires target(%d) <= current(%d)", tip.Number, s.tip.Number)
	}
	// next fails closed on a zero tip hash, so taking one here would stall the store
	// rather than report the bad argument to the caller.
	if tip.Hash == (common.Hash{}) {
		return fmt.Errorf("rewindTip requires a tip hash, got the zero hash at height %d", tip.Number)
	}
	s.tip = tip
	return nil
}

func (s *batchStore) advanceOnFinalization(finalizedL2 uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if finalizedL2 <= s.lastFinalizedL2 {
		return
	}
	s.lastFinalizedL2 = finalizedL2

	// We want to avoid deleting entries past the tip, because they have not yet been read by the
	// consumer. hotShotPos never rewinds, so a deleted batch can never be read again.
	end := min(s.tip.Number, finalizedL2)
	for height := range s.batches {
		if height <= end {
			delete(s.batches, height)
		}
	}

	s.log.Info(
		"pruned finalized slots",
		"finalizedL2", finalizedL2,
		"prunedTo", end,
		"remaining batches", len(s.batches),
	)
}
