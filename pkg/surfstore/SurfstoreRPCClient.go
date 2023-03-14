package surfstore

import (
	context "context"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	*blockHashes = append(*blockHashes, b.Hashes...)

	return nil
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := c.PutBlock(ctx, block)
	if err != nil {
		return err
	}
	*succ = s.Flag

	return nil
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		return err
	}
	*blockHashesOut = append(*blockHashesOut, b.Hashes...)

	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		// if the server is not the leader, try the next one
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			return err
		}
		// convert the map
		*serverFileInfoMap = m.FileInfoMap
		break
		// *serverFileInfoMap = make(map[string]*FileMetaData)
		// for fileName, file := range m.FileInfoMap {
		// 	(*serverFileInfoMap)[fileName] = new(FileMetaData)
		// 	(*serverFileInfoMap)[fileName].Filename = file.Filename
		// 	(*serverFileInfoMap)[fileName].Version = file.Version
		// 	(*serverFileInfoMap)[fileName].BlockHashList = file.BlockHashList
		// }
	}

	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			return err
		}
		*latestVersion = v.Version
		break
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		//	connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			return err
		}
		// convert the map
		*blockStoreMap = make(map[string][]string)
		for blockStoreAddr, blockHashes := range m.BlockStoreMap {
			(*blockStoreMap)[blockStoreAddr] = blockHashes.Hashes
		}
		break
	}

	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			return err
		}
		*blockStoreAddrs = append(*blockStoreAddrs, b.BlockStoreAddrs...)
		break
	}
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create a Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
