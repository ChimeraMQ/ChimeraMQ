//go:build !windows

package broker

// isWindowsProcessAlive is a no-op on non-Windows.
// Unix uses Signal(0) via os.Process.
func isWindowsProcessAlive(pid int) bool {
	return false
}
