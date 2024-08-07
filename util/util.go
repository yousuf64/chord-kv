package util

import (
	"crypto/sha1"
	"encoding/binary"
)

var M = 3
var RingSize uint = 9

func Hash(key string) uint64 {
	h := sha1.New()
	h.Write([]byte(key))
	b := h.Sum(nil)
	return binary.BigEndian.Uint64(b) % uint64(RingSize)
}

func Between(id, start, end uint64) bool {
	if start < end {
		return id > start && id <= end // 3 ...5 8 9... 12
	}
	return id > start || id <= end
}
