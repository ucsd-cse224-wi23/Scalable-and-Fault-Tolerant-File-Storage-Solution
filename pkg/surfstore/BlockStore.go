package surfstore

import (
	context "context"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// return the block if it exists
	if block, ok := bs.BlockMap[blockHash.Hash]; ok {
		return block, nil
	}
	// otherwise return an error
	return nil, errors.New("Block not found")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// compute the hash of the block
	hash := GetBlockHashString(block.BlockData)
	// store the block
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// create an empty list of hashes
	blockHashesOut := &BlockHashes{
		Hashes: []string{},
	}
	// iterate through the blockHashesIn
	for _, hash := range blockHashesIn.Hashes {
		// if the block exists, add it to the list
		if _, ok := bs.BlockMap[hash]; ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, hash)
		}
	}
	return blockHashesOut, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// create an empty list of hashes
	blockHashesOut := &BlockHashes{
		Hashes: []string{},
	}
	// iterate through the blockHashesIn
	for hash := range bs.BlockMap {
		blockHashesOut.Hashes = append(blockHashesOut.Hashes, hash)
	}
	return blockHashesOut, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
