// Package blockedbloom implements a blocked Bloom filter for approximate
// membership checks in memory and on disk.
//
// # Filter layout
//
// All probes for a key stay in one 64-byte block for cache locality.
// [HashKey] hashes the full key with xxHash64. Multiply-shift derives the
// probe positions from that hash. [New] estimates the number of blocks and
// probes from the expected number of distinct keys and a target false-positive
// rate.
//
// # Disk lookups
//
// [Filter.WriteTo] stores an IEEE CRC32 for the header and each 4 KiB group
// of blocks. [Open] loads the checksum table so [BlockReader.MayContain] can
// read and verify just one group per lookup.
//
// Align the filter's starting offset to 4 KiB to keep each group within one
// 4 KiB page. Skip data only when the lookup returns false and no error.
package blockedbloom
