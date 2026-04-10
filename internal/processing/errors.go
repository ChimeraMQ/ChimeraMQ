package processing

import "errors"

var (
	ErrTopologyExists   = errors.New("topology already exists")
	ErrTopologyNotFound = errors.New("topology not found")
	ErrTopologyRunning  = errors.New("topology is running; stop it first")
)
