package util

import (
	"crypto/sha1"
	"encoding/binary"
)

const M = 3
const RingSize = 8

func Hash(key string) uint64 {
	h := sha1.New()
	h.Write([]byte(key))
	b := h.Sum(nil)
	return binary.BigEndian.Uint64(b) % RingSize
}

func Between(id, start, end uint64) bool {
	if start < end {
		return id > start && id <= end // 3 ...5 8 9... 12
	}
	return id > start || id <= end
}
