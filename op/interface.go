package op

import (
	"context"
	"fmt"
	"math/big"

	espressoCommon "github.com/EspressoSystems/espresso-network/sdks/go/types"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// BatchValidity is the verdict checkBatch reaches about a batch.
type BatchValidity uint8

const (
	// BatchDrop indicates that the batch is invalid and will stay invalid, absent a reorg
	BatchDrop BatchValidity = iota
	// BatchAccept indicates that the batch is valid and should be processed
	BatchAccept
)

func (v BatchValidity) String() string {
	switch v {
	case BatchDrop:
		return "drop"
	case BatchAccept:
		return "accept"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(v))
	}
}

// DroppingBatchLogPrefix is the log message prefix used when dropping a batch.
//
// NOTE: It is referenced by the DroppingBatch constant in logmodule/log_keys.go of the
// optimism-espresso-integration repo for log investigation. Any change here must be reflected
// there too.
const DroppingBatchLogPrefix = "Dropping batch"

// HOTSHOT_BLOCK_FETCH_LIMIT is the maximum number of blocks to attempt to
// load from Espresso in a single process using fetch API.
// This helps to limit our block polling to a limited number of blocks within
// a single batched attempt.
const HOTSHOT_BLOCK_FETCH_LIMIT = 100

// EspressoClient is an interface that documents the methods we utilize for
// the espressoClient.Client.
//
// As a result we are able to easily swap implementations for testing, or
// for modification / wrapping.
type EspressoClient interface {
	FetchLatestBlockHeight(ctx context.Context) (uint64, error)
	FetchNamespaceTransactionsInRange(ctx context.Context, fromHeight uint64, toHeight uint64, namespace uint64) ([]espressoCommon.NamespaceTransactionsRangeData, error)
	FetchHeadersByRange(ctx context.Context, fromHeight uint64, toHeight uint64) ([]espressoCommon.HeaderImpl, error)
}

// L1Client is an interface that documents the methods we utilize for
// the L1 client.
type L1Client interface {
	HeaderHashByNumber(ctx context.Context, number *big.Int) (common.Hash, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	bind.ContractCaller
}

// Subset of L1 state we're interested in for a particular L1 origin block:
// the block hash, used to validate a batch's declared L1 origin.
type l1State struct {
	// Block hash
	hash common.Hash
}
