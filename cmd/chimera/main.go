package main

import (
	"fmt"
	"os"

	"github.com/chimeramq/chimera/internal/cli"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "server":
		cli.RunServer(os.Args[2:])
	case "topic":
		cli.RunTopicCLI(os.Args[2:])
	case "produce":
		cli.RunProduceCLI(os.Args[2:])
	case "consume":
		cli.RunConsumeCLI(os.Args[2:])
	case "cluster":
		cli.RunClusterCLI(os.Args[2:])
	case "wasm":
		cli.RunWASMCLI(os.Args[2:])
	case "mcp-server":
		cli.RunMCPServer(os.Args[2:])
	case "version":
		printVersion()
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf("ChimeraMQ — The Unified Messaging Beast\n")
	fmt.Printf("Three Heads. One Binary. All Messages.\n\n")
	fmt.Printf("Usage: chimera <command> [options]\n\n")
	fmt.Printf("Commands:\n")
	fmt.Printf("  server     Start the ChimeraMQ broker\n")
	fmt.Printf("  topic      Manage topics (create, list, describe, delete)\n")
	fmt.Printf("  produce    Produce messages to a topic\n")
	fmt.Printf("  consume    Consume messages from a topic\n")
	fmt.Printf("  cluster    Cluster management (status, members)\n")
	fmt.Printf("  wasm       Manage WASM modules (deploy, list, remove)\n")
	fmt.Printf("  mcp-server Start MCP server for AI tooling (JSON-RPC over stdio)\n")
	fmt.Printf("  version    Print version information\n")
}

func printVersion() {
	fmt.Printf("ChimeraMQ %s\n", version)
	fmt.Printf("  commit:  %s\n", commit)
	fmt.Printf("  built:   %s\n", date)
}
