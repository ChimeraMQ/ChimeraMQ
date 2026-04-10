package hot

import "os"

// RetentionPolicy controls when old data is deleted.
type RetentionPolicy struct {
	MaxSize     int64 // Max total bytes per partition (0 = unlimited)
	MaxSegments int   // Max frozen segments to keep (0 = unlimited)
}

// EnforceRetention removes old segments based on the policy.
// Returns the number of segments removed.
func EnforceRetention(p *Partition, policy RetentionPolicy) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	removed := 0

	// Enforce max segments
	if policy.MaxSegments > 0 {
		for len(p.segments) > 1 && countFrozen(p.segments) > policy.MaxSegments {
			// Remove oldest frozen segment
			seg := findOldestFrozen(p.segments)
			if seg == nil {
				break
			}
			removeSegment(p, seg)
			removed++
		}
	}

	// Enforce max size
	if policy.MaxSize > 0 {
		for len(p.segments) > 1 && totalSize(p.segments) > policy.MaxSize {
			seg := findOldestFrozen(p.segments)
			if seg == nil {
				break
			}
			removeSegment(p, seg)
			removed++
		}
	}

	return removed
}

func countFrozen(segments []*Segment) int {
	count := 0
	for _, seg := range segments {
		if seg.frozen {
			count++
		}
	}
	return count
}

func findOldestFrozen(segments []*Segment) *Segment {
	for _, seg := range segments {
		if seg.frozen {
			return seg
		}
	}
	return nil
}

func totalSize(segments []*Segment) int64 {
	var total int64
	for _, seg := range segments {
		total += seg.size
	}
	return total
}

func removeSegment(p *Partition, seg *Segment) {
	for i, s := range p.segments {
		if s == seg {
			// Remove from slice
			p.segments = append(p.segments[:i], p.segments[i+1:]...)
			// Update logStart
			if len(p.segments) > 0 {
				p.logStart = p.segments[0].baseOff
			}
			break
		}
	}
	// Close and delete files
	seg.mu.Lock()
	if seg.file != nil {
		seg.file.Close()
		os.Remove(seg.path)
		os.Remove(seg.path + ".idx")
	}
	seg.mu.Unlock()
}
