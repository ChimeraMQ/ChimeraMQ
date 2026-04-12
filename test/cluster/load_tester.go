// Package cluster provides cluster load testing and benchmarking tools.
package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// LoadTestConfig configures a cluster load test.
type LoadTestConfig struct {
	// Nodes is the list of admin API addresses (e.g., "127.0.0.1:9090")
	Nodes []string

	// Topic is the topic to publish to
	Topic string

	// Producers is the number of concurrent producers
	Producers int

	// MessagesPerSecond is the target aggregate message rate
	MessagesPerSecond int

	// Duration is how long to run the test
	Duration time.Duration

	// MessageSize is the approximate message size in bytes
	MessageSize int

	// BatchSize is the number of messages to batch per request
	BatchSize int
}

// LoadTestResult contains the results of a load test.
type LoadTestResult struct {
	TotalMessages  uint64
	FailedMessages uint64
	Duration       time.Duration
	MessagesPerSec float64
	AvgLatencyMs   float64
	P99LatencyMs   float64
	Errors         []error
}

// LoadTester runs load tests against a ChimeraMQ cluster.
type LoadTester struct {
	config LoadTestConfig
	client *http.Client
}

// NewLoadTester creates a new cluster load tester.
func NewLoadTester(config LoadTestConfig) *LoadTester {
	return &LoadTester{
		config: config,
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Run executes the load test.
func (lt *LoadTester) Run() (*LoadTestResult, error) {
	if len(lt.config.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes configured")
	}

	var (
		totalMessages  atomic.Uint64
		failedMessages atomic.Uint64
		latencies      = make(chan time.Duration, 10000)
		stopCh         = make(chan struct{})
		errors         = make([]error, 0)
		errorsMu       sync.Mutex
	)

	// Latency collector
	var latencyList []time.Duration
	var latencyMu sync.Mutex
	go func() {
		for lat := range latencies {
			latencyMu.Lock()
			latencyList = append(latencyList, lat)
			latencyMu.Unlock()
		}
	}()

	// Start producers
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < lt.config.Producers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			lt.producer(producerID, &totalMessages, &failedMessages, latencies, stopCh, &errors, &errorsMu)
		}(i)
	}

	// Wait for test duration
	time.Sleep(lt.config.Duration)
	close(stopCh)
	wg.Wait()
	close(latencies)

	// Wait for latency collector
	time.Sleep(100 * time.Millisecond)

	duration := time.Since(startTime)

	// Calculate statistics
	result := &LoadTestResult{
		TotalMessages:  totalMessages.Load(),
		FailedMessages: failedMessages.Load(),
		Duration:       duration,
		MessagesPerSec: float64(totalMessages.Load()) / duration.Seconds(),
		Errors:         errors,
	}

	// Calculate latency statistics
	if len(latencyList) > 0 {
		var totalLatency time.Duration
		for _, lat := range latencyList {
			totalLatency += lat
		}
		result.AvgLatencyMs = float64(totalLatency.Milliseconds()) / float64(len(latencyList))

		// Calculate P99
		sorted := make([]time.Duration, len(latencyList))
		copy(sorted, latencyList)
		// Simple sort (for production, use a more efficient algorithm)
		for i := 0; i < len(sorted); i++ {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[j] < sorted[i] {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		p99Idx := int(float64(len(sorted)) * 0.99)
		if p99Idx >= len(sorted) {
			p99Idx = len(sorted) - 1
		}
		result.P99LatencyMs = float64(sorted[p99Idx].Milliseconds())
	}

	return result, nil
}

func (lt *LoadTester) producer(id int, totalMessages, failedMessages *atomic.Uint64,
	latencies chan<- time.Duration, stopCh <-chan struct{},
	errors *[]error, errorsMu *sync.Mutex) {

	// Round-robin across nodes
	nodeIdx := id % len(lt.config.Nodes)
	node := lt.config.Nodes[nodeIdx]

	// Calculate delay between messages
	var delay time.Duration
	if lt.config.MessagesPerSecond > 0 && lt.config.Producers > 0 {
		messagesPerProducer := lt.config.MessagesPerSecond / lt.config.Producers
		if messagesPerProducer > 0 {
			delay = time.Second / time.Duration(messagesPerProducer)
		}
	}

	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	seq := uint64(0)
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			// Generate message
			msg := lt.generateMessage(id, seq)
			seq++

			// Send message
			start := time.Now()
			err := lt.sendMessage(node, lt.config.Topic, msg)
			latency := time.Since(start)

			if err != nil {
				failedMessages.Add(1)
				errorsMu.Lock()
				if len(*errors) < 100 { // Limit stored errors
					*errors = append(*errors, err)
				}
				errorsMu.Unlock()
			} else {
				totalMessages.Add(1)
				if len(latencies) < cap(latencies) {
					latencies <- latency
				}
			}
		}
	}
}

func (lt *LoadTester) generateMessage(producerID int, seq uint64) map[string]interface{} {
	return map[string]interface{}{
		"producer":  producerID,
		"seq":       seq,
		"timestamp": time.Now().UnixNano(),
		"data":      make([]byte, lt.config.MessageSize),
	}
}

func (lt *LoadTester) sendMessage(node, topic string, msg map[string]interface{}) error {
	url := fmt.Sprintf("http://%s/v1/messages/%s", node, topic)

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := lt.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// PrintResults prints load test results to stdout.
func (result *LoadTestResult) PrintResults() {
	fmt.Println("=== Cluster Load Test Results ===")
	fmt.Printf("Duration:         %v\n", result.Duration)
	fmt.Printf("Total Messages:   %d\n", result.TotalMessages)
	fmt.Printf("Failed Messages:  %d\n", result.FailedMessages)
	fmt.Printf("Messages/sec:     %.2f\n", result.MessagesPerSec)
	fmt.Printf("Avg Latency:      %.2f ms\n", result.AvgLatencyMs)
	fmt.Printf("P99 Latency:      %.2f ms\n", result.P99LatencyMs)
	fmt.Printf("Success Rate:     %.2f%%\n",
		100.0*float64(result.TotalMessages)/float64(result.TotalMessages+result.FailedMessages))

	if len(result.Errors) > 0 {
		fmt.Printf("\nSample Errors (%d total):\n", len(result.Errors))
		for i, err := range result.Errors {
			if i >= 5 {
				break
			}
			fmt.Printf("  - %v\n", err)
		}
	}
}

// ClusterHealth checks the health of a cluster.
func ClusterHealth(nodes []string) map[string]error {
	results := make(map[string]error)
	client := &http.Client{Timeout: 5 * time.Second}

	for _, node := range nodes {
		url := fmt.Sprintf("http://%s/v1/health", node)
		resp, err := client.Get(url)
		if err != nil {
			results[node] = err
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			results[node] = fmt.Errorf("HTTP %d", resp.StatusCode)
		} else {
			results[node] = nil
		}
	}

	return results
}

// WaitForCluster waits for all nodes in a cluster to become healthy.
func WaitForCluster(nodes []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		health := ClusterHealth(nodes)
		allHealthy := true
		for _, err := range health {
			if err != nil {
				allHealthy = false
				break
			}
		}
		if allHealthy {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for cluster to become healthy")
}
