//go:build windows

package broker

import (
	"syscall"
)

// isWindowsProcessAlive checks if a process is alive using OpenProcess on Windows.
func isWindowsProcessAlive(pid int) bool {
	h, err := syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)

	var exitCode uint32
	err = syscall.GetExitCodeProcess(h, &exitCode)
	if err != nil {
		return false
	}
	// STILL_ACTIVE = 259
	return exitCode == 259
}
