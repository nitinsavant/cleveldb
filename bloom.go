package main

import (
	"encoding/binary"
	"hash/fnv"
	"log"
)

type BloomFilter struct {
	data      []byte
	size      int // in bytes
	numHashes int
}

func newBloomFilter(size int, numHashes int) *BloomFilter {
	return &BloomFilter{
		data:      make([]byte, size),
		size:      size,
		numHashes: numHashes,
	}
}

func (b *BloomFilter) Add(item []byte) {
	var bitIdx, targetByteIdx, byteBitIdx int

	for i := 0; i < b.numHashes; i++ {
		// The index of the bit to flip on
		bitIdx = b.hash(append(item, byte(i))) % (b.size * 8)
		targetByteIdx = bitIdx / 8
		byteBitIdx = bitIdx % 8

		// Set the bit within the target byte (and update the byte slice)
		b.data[targetByteIdx] = setBit(b.data[targetByteIdx], uint(7-byteBitIdx))
	}
}

func (b *BloomFilter) hash(item []byte) int {
	h := fnv.New32a()

	_, err := h.Write(item)
	if err != nil {
		log.Fatal(err)
	}

	return int(h.Sum32())
}

func (b *BloomFilter) MaybeContains(item []byte) bool {
	var bitIdx, targetByteIdx, byteBitIdx int

	for i := 0; i < b.numHashes; i++ {
		// The index of the bit that should be on
		bitIdx = b.hash(append(item, byte(i))) % (b.size * 8)

		// The index of the byte that contains the bit (that should be on)
		targetByteIdx = bitIdx / 8

		// The index of the bit within the byte (that contains the bit that should be on)
		byteBitIdx = bitIdx % 8

		if !hasBit(b.data[targetByteIdx], uint(7-byteBitIdx)) {
			return false
		}
	}

	return true
}

func (b *BloomFilter) MemoryUsage() int {
	return binary.Size(b.data)
}

func setBit(n byte, pos uint) byte {
	n |= 1 << pos
	return n
}

func hasBit(n byte, pos uint) bool {
	val := n & (1 << pos)
	return val > 0
}
