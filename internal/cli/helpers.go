package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// httpClient is a configured HTTP client with timeouts
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
}

func getAdminAddr() string {
	if addr := os.Getenv("CHIMERA_ADMIN_ADDR"); addr != "" {
		// Add http:// prefix if not present
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			return "http://" + addr
		}
		return addr
	}
	return "http://localhost:9090"
}

func httpGet(url string) (*http.Response, error) {
	return httpClient.Get(url)
}

func httpPost(url string, body []byte) (*http.Response, error) {
	return httpClient.Post(url, "application/json", bytes.NewReader(body))
}

func httpDelete(url string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	return httpClient.Do(req)
}

func printResponse(resp *http.Response) {
	var pretty bytes.Buffer
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	_ = json.Indent(&pretty, body, "", "  ")
	fmt.Println(pretty.String())
}

func readStdin() []byte {
	data, _ := io.ReadAll(io.LimitReader(os.Stdin, 16*1024*1024))
	return data
}

// RunReloadCLI triggers configuration reload via admin API.
func RunReloadCLI(args []string) {
	adminAddr := getAdminAddr()

	resp, err := httpClient.Post(adminAddr+"/v1/config/reload", "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		os.Exit(1)
	}

	printResponse(resp)
}
