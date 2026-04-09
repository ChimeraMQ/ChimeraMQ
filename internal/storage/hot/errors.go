package hot

import "errors"

var (
	ErrSegmentFull         = errors.New("segment is full")
	ErrPositionOutOfBounds = errors.New("position out of bounds")
	ErrOffsetTooOld        = errors.New("offset too old")
	ErrSegmentReadOnly     = errors.New("segment is read-only")
	ErrBadMagic            = errors.New("segment file has invalid magic bytes")
)
