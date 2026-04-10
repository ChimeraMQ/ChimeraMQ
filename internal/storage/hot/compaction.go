package hot

import "time"

// CompactionMode controls how log compaction works.
type CompactionMode int

const (
	CompactNone     CompactionMode = iota
	CompactKeyBased                // Keep latest value per key
)

// LogCompactor performs log compaction on a partition.
type LogCompactor struct {
	mode     CompactionMode
	interval time.Duration
}

// NewLogCompactor creates a new compactor.
func NewLogCompactor(mode CompactionMode, interval time.Duration) *LogCompactor {
	return &LogCompactor{mode: mode, interval: interval}
}

// ShouldCompact returns true if the partition has enough frozen segments to justify compaction.
func (lc *LogCompactor) ShouldCompact(p *Partition) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	frozen := 0
	for _, seg := range p.segments {
		if seg.frozen {
			frozen++
		}
	}
	return frozen >= 2
}
