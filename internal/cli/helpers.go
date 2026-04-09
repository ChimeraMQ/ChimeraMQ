package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

func getAdminAddr() string {
	if addr := os.Getenv("CHIMERA_ADMIN_ADDR"); addr != "" {
		return addr
	}
	return "http://localhost:9090"
}

func httpGet(url string) (*http.Response, error) {
	return http.Get(url)
}

func httpPost(url string, body []byte) (*http.Response, error) {
	return http.Post(url, "application/json", bytes.NewReader(body))
}

func httpDelete(url string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func printResponse(resp *http.Response) {
	var pretty bytes.Buffer
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	json.Indent(&pretty, body, "", "  ")
	fmt.Println(pretty.String())
}

func readStdin() []byte {
	data, _ := io.ReadAll(io.LimitReader(os.Stdin, 16*1024*1024))
	return data
}
