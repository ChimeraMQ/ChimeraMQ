package warm

import "errors"

var (
	errBadBloomData  = errors.New("invalid bloom filter data")
	errBadBlockIndex = errors.New("invalid block index data")
	errBadSSTable    = errors.New("invalid sstable file")
	errLSMClosed     = errors.New("lsm tree is closed")
)
