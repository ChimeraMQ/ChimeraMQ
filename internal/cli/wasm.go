package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

// RunWASMCLI handles the wasm subcommand.
func RunWASMCLI(args []string) {
	if len(args) < 1 {
		printWASMUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "deploy":
		runWASMDeploy(args[1:])
	case "list":
		runWASMList()
	case "remove":
		runWASMRemove(args[1:])
	default:
		printWASMUsage()
		os.Exit(1)
	}
}

func printWASMUsage() {
	fmt.Printf("Usage: chimera wasm <command> [options]\n\n")
	fmt.Printf("Commands:\n")
	fmt.Printf("  deploy <module.wasm> [--name <name>]  Upload and compile a WASM module\n")
	fmt.Printf("  list                                  List compiled WASM modules\n")
	fmt.Printf("  remove <name>                         Remove a WASM module\n")
}

func runWASMDeploy(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: chimera wasm deploy <module.wasm> [--name <name>]")
		os.Exit(1)
	}

	path := args[0]
	name := ""
	for i := 1; i < len(args)-1; i++ {
		if args[i] == "--name" {
			name = args[i+1]
			break
		}
	}
	if name == "" {
		name = path
	}

	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", path, err)
		os.Exit(1)
	}

	url := fmt.Sprintf("%s/v1/wasm/modules?name=%s", getAdminAddr(), url.QueryEscape(name))
	resp, err := httpClient.Post(url, "application/wasm", bytes.NewReader(data))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode != http.StatusCreated {
		fmt.Fprintf(os.Stderr, "Error (%d): %s\n", resp.StatusCode, body)
		os.Exit(1)
	}

	var result map[string]string
	_ = json.Unmarshal(body, &result)
	fmt.Printf("Module %q compiled successfully\n", result["name"])
}

func runWASMList() {
	url := fmt.Sprintf("%s/v1/wasm/modules", getAdminAddr())
	resp, err := httpClient.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	var result map[string]interface{}
	_ = json.Unmarshal(body, &result)

	modules, _ := result["modules"].([]interface{})
	if len(modules) == 0 {
		fmt.Println("No WASM modules loaded.")
		return
	}

	fmt.Printf("WASM Modules (%d):\n", len(modules))
	for _, m := range modules {
		fmt.Printf("  - %s\n", m)
	}
}

func runWASMRemove(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: chimera wasm remove <name>")
		os.Exit(1)
	}

	name := args[0]
	url := fmt.Sprintf("%s/v1/wasm/modules/%s", getAdminAddr(), url.PathEscape(name))
	resp, err := httpDelete(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		fmt.Printf("Module %q removed\n", name)
	} else {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		fmt.Fprintf(os.Stderr, "Error: %s\n", body)
		os.Exit(1)
	}
}
