package warm

import "errors"

var (
	errBadBloomData  = errors.New("invalid bloom filter data")
	errBadBlockIndex = errors.New("invalid block index data")
	errSSTableClosed = errors.New("sstable is closed")
	errBadSSTable    = errors.New("invalid sstable file")
	errLSMClosed     = errors.New("lsm tree is closed")
)
