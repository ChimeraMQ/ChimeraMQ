package cli

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

// RunUpgradeCLI runs the rolling upgrade command.
func RunUpgradeCLI(args []string) {
	fs := flag.NewFlagSet("upgrade", flag.ExitOnError)
	action := fs.String("action", "status", "Action: status, drain, or wait")
	dataDir := fs.String("data-dir", "/var/lib/chimera", "Data directory (for handoff socket)")
	timeout := fs.Duration("timeout", 30*time.Second, "Timeout for drain/wait operations")
	verbose := fs.Bool("v", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	handoffSock := *dataDir + "/handoff.sock"

	switch *action {
	case "status":
		status, err := getHandoffStatus(handoffSock)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Handoff Status: %s\n", status)

	case "drain":
		if *verbose {
			fmt.Printf("Requesting connection drain via %s...\n", handoffSock)
		}
		err := sendHandoffCommand(handoffSock, "DRAI", *timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Drain failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Drain initiated successfully")

	case "wait":
		if *verbose {
			fmt.Printf("Waiting for handoff signal (timeout: %v)...\n", *timeout)
		}
		// Connect to handoff socket and wait for drain signal
		conn, err := net.DialTimeout("unix", handoffSock, *timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot connect to handoff socket: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close()

		// Set read timeout
		_ = conn.SetReadDeadline(time.Now().Add(*timeout))

		// Wait for DRAIN command from new version
		buf := make([]byte, 4)
		if _, err := conn.Read(buf); err != nil {
			fmt.Fprintf(os.Stderr, "Timeout waiting for handoff: %v\n", err)
			os.Exit(1)
		}

		if string(buf) == "DRAI" {
			if *verbose {
				fmt.Println("Received handoff signal, initiating drain...")
			}
			// Send acknowledgment
			_, _ = conn.Write([]byte("ACK "))
			fmt.Println("Handoff initiated successfully")
		} else {
			fmt.Fprintf(os.Stderr, "Unexpected handoff signal: %s\n", string(buf))
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown action: %s\n", *action)
		fs.Usage()
		os.Exit(1)
	}
}

// getHandoffStatus queries the handoff status from the running broker.
func getHandoffStatus(handoffSock string) (string, error) {
	conn, err := net.DialTimeout("unix", handoffSock, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("connect to handoff socket: %w", err)
	}
	defer conn.Close()

	// Send STAT command
	if _, err := conn.Write([]byte("STAT")); err != nil {
		return "", fmt.Errorf("send status command: %w", err)
	}

	// Read response
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	return string(buf[:n]), nil
}

// sendHandoffCommand sends a command to the handoff socket.
func sendHandoffCommand(handoffSock string, cmd string, timeout time.Duration) error {
	conn, err := net.DialTimeout("unix", handoffSock, timeout)
	if err != nil {
		return fmt.Errorf("connect to handoff socket: %w", err)
	}
	defer conn.Close()

	// Set deadline for the entire operation
	_ = conn.SetDeadline(time.Now().Add(timeout))

	// Send command
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	// Read response
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	response := string(buf[:n])
	if len(response) < 4 {
		return fmt.Errorf("invalid response: %s", response)
	}

	status := response[:4]
	if status == "ERR " {
		return fmt.Errorf("server error: %s", response[4:])
	}
	if status != "OK  " {
		return fmt.Errorf("unexpected response: %s", response)
	}

	return nil
}
