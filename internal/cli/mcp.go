package cli

import (
	"fmt"
	"os"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/mcp"
)

// RunMCPServer starts the MCP server using the broker config.
func RunMCPServer(args []string) {
	configPath := ""
	if len(args) > 0 {
		for i, arg := range args {
			if arg == "--config" && i+1 < len(args) {
				configPath = args[i+1]
			}
		}
	}

	cfg, err := broker.LoadConfig(configPath, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading config: %v\n", err)
		os.Exit(1)
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating broker: %v\n", err)
		os.Exit(1)
	}

	if err := b.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	server := mcp.NewServer(b)
	if err := server.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "MCP server error: %v\n", err)
		os.Exit(1)
	}
}
