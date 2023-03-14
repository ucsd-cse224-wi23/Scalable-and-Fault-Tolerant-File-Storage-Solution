package surfstore

import (
	context "context"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// return the file info map
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// get the file name
	fileName := fileMetaData.Filename
	fileVersion := fileMetaData.Version
	// check if the file exists
	if _, ok := m.FileMetaMap[fileName]; ok {
		// if the file exists, check if the new version is exactly one more than the old version
		if m.FileMetaMap[fileName].Version == fileVersion-1 {
			// if the version is correct, update the file info
			m.FileMetaMap[fileName] = fileMetaData
			return &Version{
				Version: fileVersion,
			}, nil
		}
		// otherwise return version -1
		return &Version{
			Version: -1,
		}, nil
	}
	// if the file does not exist, check if the version is 1
	if fileVersion == 1 {
		m.FileMetaMap[fileName] = fileMetaData
		return &Version{
			Version: fileVersion,
		}, nil
	}
	// otherwise return error file not found
	return nil, errors.New("file not found")
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// Consistent Hashing
	blockStoreMap := make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		blockStoreAddr := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := blockStoreMap[blockStoreAddr]; !ok {
			blockStoreMap[blockStoreAddr] = &BlockHashes{
				Hashes: []string{},
			}
		}
		blockStoreMap[blockStoreAddr] = &BlockHashes{
			Hashes: append(blockStoreMap[blockStoreAddr].Hashes, blockHash),
		}
	}
	return &BlockStoreMap{
		BlockStoreMap: blockStoreMap,
	}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{
		BlockStoreAddrs: m.BlockStoreAddrs,
	}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
