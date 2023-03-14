package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// function to get the responsible server for a block
	// get the hash of the block
	blockHash := blockId
	// sort the hashes in the ring
	hashes := []string{}
	for hash := range c.ServerMap {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)

	// iterate through the hashes in the ring
	for _, hash := range hashes {
		// if the hash is greater than the block hash, return the server
		if hash >= blockHash {
			return c.ServerMap[hash]
		}
	}
	// otherwise return the first server
	return c.ServerMap[hashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// function to create a new consistent hash ring
	consistentHashRing := &ConsistentHashRing{
		ServerMap: map[string]string{},
	}
	// add each server to the ring
	for _, serverAddr := range serverAddrs {
		log.Println("blockstore" + serverAddr)
		consistentHashRing.ServerMap[consistentHashRing.Hash("blockstore"+serverAddr)] = serverAddr
	}
	return consistentHashRing
}
