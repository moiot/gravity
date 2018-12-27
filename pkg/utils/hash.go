package utils

import "hash/crc32"

// GenHashKey generates key with crc32 algorithm
func GenHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
