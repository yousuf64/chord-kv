package chord

import (
	"crypto/sha1"
	"encoding/binary"
)

type Hasher func(key string) uint64

func GenHasher(ringSize uint64) Hasher {
	return func(key string) uint64 {
		h := sha1.New()
		h.Write([]byte(key))
		b := h.Sum(nil)
		return binary.BigEndian.Uint64(b) % ringSize
	}
}
