package broker

import (
	"hash/fnv"
)

// getPartition returns the partition index for a given key.
// It uses FNV-1a hash to distribute keys across partitions.
func getPartition(key []byte, numPartitions int32) int32 {
	if numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(numPartitions))
}
