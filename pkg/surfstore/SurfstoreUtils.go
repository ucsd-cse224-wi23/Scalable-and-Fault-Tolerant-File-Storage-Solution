package surfstore

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

// Get block hashes of a file
func GetBlockHashes(fileName string, baseDir string, blockSize int) ([]string, error) {
	// get the absolute file path
	filePath, err := filepath.Abs(filepath.Join(baseDir, fileName))
	if err != nil {
		return nil, err
	}
	// read the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// get the file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return []string{EMPTYFILE_HASHVALUE}, nil
	}
	// get the number of blocks
	numBlocks := (int(fileSize) + blockSize - 1) / blockSize
	// get the block hashes
	blockHashes := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := make([]byte, blockSize)
		n, err := file.Read(block)
		if err != nil {
			return nil, err
		}
		blockHashes[i] = GetBlockHashString(block[:n])
	}
	return blockHashes, nil
}

// Put file onto Block Store
func PutFileToBlockStore(fileName string, client RPCClient) error {
	// get the absolute file path
	filePath, err := filepath.Abs(filepath.Join(client.BaseDir, fileName))
	if err != nil {
		return err
	}
	// read the file
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// get the file size
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	// get the number of blocks
	numBlocks := (int(fileSize) + client.BlockSize - 1) / client.BlockSize
	log.Println("numBlocks: ", numBlocks)
	log.Println("fileSize: ", fileSize, " blockSize: ", client.BlockSize, " numBlocks: ", numBlocks)
	// put the blocks onto the block store
	for i := 0; i < numBlocks; i++ {
		buff := make([]byte, client.BlockSize)
		n, err := file.Read(buff)
		if err != nil {
			return err
		}
		block := new(Block)
		block.BlockData = buff[:n]
		block.BlockSize = int32(n)
		blockHash := GetBlockHashString(block.BlockData)
		blockStoreMap := new(map[string][]string)
		err = client.GetBlockStoreMap([]string{blockHash}, blockStoreMap)
		// check if the map is empty
		if err != nil {
			return err
		}
		blockStoreAddr := ""
		for _blockStoreAddr := range *blockStoreMap {
			blockStoreAddr = _blockStoreAddr
		}

		succ := new(bool)
		e := client.PutBlock(block, blockStoreAddr, succ)
		if e != nil {
			return e
		}
	}
	return nil

}

// Put file meta data onto Meta Store
func PutFileToMetaStore(fileName string, version int32, client RPCClient) error {
	// get the file info
	_, err := os.Stat(filepath.Join(client.BaseDir, fileName))
	if err != nil {
		return err
	}
	// get the block hashes
	blockHashes, err := GetBlockHashes(fileName, client.BaseDir, client.BlockSize)
	if err != nil {
		return err
	}
	// create the file meta data
	fileMeta := new(FileMetaData)
	fileMeta.Filename = fileName
	fileMeta.Version = version
	fileMeta.BlockHashList = blockHashes

	latestVersion := new(int32)
	e := client.UpdateFile(fileMeta, latestVersion)
	if e != nil {
		return e
	}
	if (*latestVersion) != version {
		return errors.New("Version Mismatch")
	}
	return nil
}

// Download file from Block Store
func DownloadFileFromBlockStore(fileName string, client RPCClient) error {
	log.Println(fileName)
	// get the absolute file path
	filePath, err := filepath.Abs(filepath.Join(client.BaseDir, fileName))
	if err != nil {
		return err
	}

	// get the file info map from the meta store
	fileInfoMap := new(map[string]*FileMetaData)
	e := client.GetFileInfoMap(fileInfoMap)
	if e != nil {
		return e
	}
	// get the file meta data
	fileMetaRemote, ok := (*fileInfoMap)[fileName]
	if !ok {
		return errors.New("file not found")
	}

	// create the file
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// blockStoreAddr := new(string)
	// e = client.GetBlockStoreAddr(blockStoreAddr)
	// if e != nil {
	// 	return e
	// }
	// check if the file is deleted
	if len(fileMetaRemote.BlockHashList) == 1 && fileMetaRemote.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		// remove the file as the file is deleted in remote
		e = os.Remove(filePath)
		if e != nil {
			return e
		}
		return nil
	}

	// check if it is an empty file
	if len(fileMetaRemote.BlockHashList) == 1 && fileMetaRemote.BlockHashList[0] == EMPTYFILE_HASHVALUE {
		return nil
	}

	for _, blockHash := range fileMetaRemote.BlockHashList {
		block := new(Block)
		blockStoreMap := new(map[string][]string)
		client.GetBlockStoreMap([]string{blockHash}, blockStoreMap)
		// check if the map is empty
		if len(*blockStoreMap) == 0 {
			return errors.New("Block store map is empty")
		}
		blockStoreAddr := ""
		for _blockStoreAddr := range *blockStoreMap {
			blockStoreAddr = _blockStoreAddr
		}
		log.Println("blockStoreAddr: ", blockStoreAddr)
		log.Println("blockHash: ", blockHash)
		err := client.GetBlock(blockHash, blockStoreAddr, block)
		if err != nil {
			return err
		}
		log.Println("Block successfully downloaded")
		_, err = file.Write(block.BlockData)
		if err != nil {
			return err
		}
	}
	return nil
}

// Put file to the Block Store and Meta Store
func PutFileToBothServers(fileName string, version int32, client RPCClient, localFileInfoMap *map[string]*FileMetaData) error {
	e := PutFileToBlockStore(fileName, client)
	if e != nil {
		return e
	}
	e = PutFileToMetaStore(fileName, version, client)
	if e != nil {
		if e.Error() == "Version Mismatch" {
			// if the version is mismatch, download the file
			e = DownloadFileAndUpdateLocalMeta(fileName, client, localFileInfoMap)
			if e != nil {
				return e
			}
		} else {
			return e
		}
	} else {
		// update the local file info map
		(*localFileInfoMap)[fileName] = new(FileMetaData)
		(*localFileInfoMap)[fileName].Filename = fileName
		(*localFileInfoMap)[fileName].Version = version
		(*localFileInfoMap)[fileName].BlockHashList, e = GetBlockHashes(fileName, client.BaseDir, client.BlockSize)
		if e != nil {
			return e
		}
	}
	return nil
}

// Download file and update the local file info map
func DownloadFileAndUpdateLocalMeta(fileName string, client RPCClient, localFileInfoMap *map[string]*FileMetaData) error {
	e := DownloadFileFromBlockStore(fileName, client)
	if e != nil {
		return e
	}
	remoteFileInfoMap := new(map[string]*FileMetaData)
	client.GetFileInfoMap(remoteFileInfoMap)
	// update the local file info map
	(*localFileInfoMap)[fileName] = new(FileMetaData)
	(*localFileInfoMap)[fileName].Filename = (*remoteFileInfoMap)[fileName].Filename
	(*localFileInfoMap)[fileName].Version = (*remoteFileInfoMap)[fileName].Version
	(*localFileInfoMap)[fileName].BlockHashList = (*remoteFileInfoMap)[fileName].BlockHashList
	return nil
}

// Delete file from Meta Store
func DeleteFileFromMetaStore(fileName string, version int32, client RPCClient) error {
	// create the file meta data
	fileMeta := new(FileMetaData)
	fileMeta.Filename = fileName
	fileMeta.Version = version
	fileMeta.BlockHashList = []string{TOMBSTONE_HASHVALUE}

	latestVersion := new(int32)
	e := client.UpdateFile(fileMeta, latestVersion)
	if e != nil {
		return e
	}
	if (*latestVersion) != version {
		return errors.New("Version Mismatch")
	}
	return nil
}

// Delete file from Meta Store and Update the local file info map
func DeleteFileFromMetaStoreAndUpdateLocalMeta(fileName string, version int32, client RPCClient, localFileInfoMap *map[string]*FileMetaData) error {
	e := DeleteFileFromMetaStore(fileName, version, client)
	if e != nil {
		if e.Error() == "Version Mismatch" {
			// if the version is mismatch, download the file
			e = DownloadFileAndUpdateLocalMeta(fileName, client, localFileInfoMap)
			if e != nil {
				return e
			}
		} else {
			return e
		}
	} else {
		// update the local file info map
		(*localFileInfoMap)[fileName] = new(FileMetaData)
		(*localFileInfoMap)[fileName].Filename = fileName
		(*localFileInfoMap)[fileName].Version = version
		(*localFileInfoMap)[fileName].BlockHashList = []string{TOMBSTONE_HASHVALUE}
	}
	return nil
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// load the local file info map
	localFileInfoMap, e := LoadMetaFromMetaFile(client.BaseDir)
	if e != nil {
		log.Fatal("Error When Loading Local Meta")
	}
	remoteFileInfoMap := new(map[string]*FileMetaData)
	// get the remote file info map
	e = client.GetFileInfoMap(remoteFileInfoMap)
	if e != nil {
		log.Fatal(e, "Error When Getting Remote Meta")
	}
	// iterate through the local file info map
	files, _ := ioutil.ReadDir(client.BaseDir)
	for _, f := range files {
		if !f.IsDir() {
			fileName := f.Name()
			if fileName != DEFAULT_META_FILENAME {
				// check if the file is in the local file info map
				fileMetaLocal, ok1 := localFileInfoMap[fileName]
				if !ok1 {
					// check if the file is in the remote file info map
					_, ok2 := (*remoteFileInfoMap)[fileName]
					if !ok2 {
						// if the file is not in the local and remote file info map, create a new file info
						e = PutFileToBothServers(fileName, 1, client, &localFileInfoMap)
						if e != nil {
							log.Fatal(e, "Error When Putting File To Both Servers")
						}
					} else {
						// if the file is not in the local file info map but in the remote file info map, download the file
						log.Println("File Not In Local Meta, Downloading File")
						e = DownloadFileAndUpdateLocalMeta(fileName, client, &localFileInfoMap)
						if e != nil {
							log.Fatal(e, "Error When Downloading File")
						}
					}
				} else {
					// check if the block hashes of the local file info map is same as the local file
					blockHashes, e := GetBlockHashes(fileName, client.BaseDir, client.BlockSize)
					if e != nil {
						log.Fatal("Error When Getting Block Hashes")
					}
					localUpdate := false
					if !sameStringSlice(blockHashes, fileMetaLocal.BlockHashList) {
						localUpdate = true
					}

					fileMetaRemote, ok2 := (*remoteFileInfoMap)[fileName]
					if !ok2 {
						// if the file is in the local file info map but not in the remote file info map, put the file to the server
						e = PutFileToBothServers(fileName, fileMetaLocal.Version+1, client, &localFileInfoMap)
						if e != nil {
							log.Fatal(e, "Error When Putting File To Both Servers")
						}
					} else {
						// if the file is in the remote file info map, check if the version is the same
						if fileMetaLocal.Version == fileMetaRemote.Version {
							if localUpdate {
								// if the version is the same, check if the local file is updated
								// if the local file is updated, put the file to the server
								e = PutFileToBothServers(fileName, fileMetaLocal.Version+1, client, &localFileInfoMap)
								if e != nil {
									log.Fatal(e, "Error When Putting File To Both Servers")
								}
							}
						} else {
							// if the version is not the same, download the file
							e = DownloadFileAndUpdateLocalMeta(fileName, client, &localFileInfoMap)
							if e != nil {
								log.Fatal(e, "Error When Downloading File")
							}
						}
					}

				}
			}
		}
	}

	// make a set of the file names present in remote file info map and local file info map but not in local directory
	FileSetWithoutLocal := make(map[string]bool)
	for fileName := range *remoteFileInfoMap {
		FileSetWithoutLocal[fileName] = true
	}
	for fileName := range localFileInfoMap {
		FileSetWithoutLocal[fileName] = true
	}
	for _, f := range files {
		if !f.IsDir() {
			fileName := f.Name()
			if fileName != DEFAULT_META_FILENAME {
				delete(FileSetWithoutLocal, fileName)
			}
		}
	}
	for fileName := range FileSetWithoutLocal {
		// check if the file is in the local file info map
		fileMetaLocal, ok1 := localFileInfoMap[fileName]
		fileMetaRemote, ok2 := (*remoteFileInfoMap)[fileName]
		if ok1 && ok2 {
			if fileMetaLocal.Version == fileMetaRemote.Version {
				// if the file is in the local file info map and remote file info map, check if the version is the same
				if len(fileMetaLocal.BlockHashList) != 1 || fileMetaLocal.BlockHashList[0] != TOMBSTONE_HASHVALUE {
					// if the version is the same, check if the block hashes is not {"0"}
					// delete the file from the server
					e = DeleteFileFromMetaStoreAndUpdateLocalMeta(fileName, fileMetaLocal.Version+1, client, &localFileInfoMap)
					if e != nil {
						log.Fatal("Error When Deleting File From Both Servers")
					}
				}
			} else {
				// if the version is not the same, download the file from server
				e = DownloadFileAndUpdateLocalMeta(fileName, client, &localFileInfoMap)
				if e != nil {
					log.Fatal(e, "Error When Downloading File")
				}
			}
		} else if ok1 {
			log.Fatal("File Not In Remote Meta")
		} else if ok2 {
			e := DownloadFileAndUpdateLocalMeta(fileName, client, &localFileInfoMap)
			if e != nil {
				log.Fatal(e, "Error When Downloading File")
			}
		}
	}

	// PrintMetaMap(localFileInfoMap)
	e = WriteMetaFile(localFileInfoMap, client.BaseDir)
	if e != nil {
		log.Fatal("Error When Writing Meta File")
	}

}

func sameStringSlice(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	// create a map of string -> int
	diff := make(map[string]int, len(x))
	for _, _x := range x {
		// 0 value for int is 0, so just increment a counter for the string
		diff[_x]++
	}
	for _, _y := range y {
		// If the string _y is not in diff bail out early
		if _, ok := diff[_y]; !ok {
			return false
		}
		diff[_y] -= 1
		if diff[_y] == 0 {
			delete(diff, _y)
		}
	}
	return len(diff) == 0
}
