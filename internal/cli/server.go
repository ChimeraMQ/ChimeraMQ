package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/protocol"
	"github.com/chimeramq/chimera/internal/protocol/amqp"
	"github.com/chimeramq/chimera/internal/protocol/chimera"
	httpadmin "github.com/chimeramq/chimera/internal/protocol/http"
	"github.com/chimeramq/chimera/internal/protocol/mqtt"
	"github.com/chimeramq/chimera/internal/protocol/ws"
)

// RunClusterCLI manages cluster operations via the admin API.
func RunClusterCLI(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: chimera cluster [status|members]")
		os.Exit(1)
	}

	adminAddr := getAdminAddr()

	switch args[0] {
	case "status":
		resp, err := httpGet(adminAddr + "/v1/health")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printResponse(resp)

	case "members":
		resp, err := httpGet(adminAddr + "/v1/cluster/members")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printResponse(resp)

	default:
		fmt.Printf("Unknown cluster command: %s\n", args[0])
		fmt.Println("Usage: chimera cluster [status|members]")
		os.Exit(1)
	}
}

// RunServer starts the ChimeraMQ broker.
func RunServer(args []string) {
	flags := flag.NewFlagSet("server", flag.ExitOnError)
	configPath := flags.String("config", "", "Path to chimera.yaml")
	dataDir := flags.String("data-dir", "", "Data directory override")
	bindAddr := flags.String("bind", "", "Bind address override")
	port := flags.Int("port", 0, "Port override")
	adminPort := flags.Int("admin-port", 0, "Admin port override")
	logLevel := flags.String("log-level", "", "Log level override")
	flags.Parse(args)

	cliFlags := &broker.CLIFlags{
		DataDir:   *dataDir,
		Bind:      *bindAddr,
		Port:      *port,
		AdminPort: *adminPort,
		LogLevel:  *logLevel,
	}

	cfg, err := broker.LoadConfig(*configPath, cliFlags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := b.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Create protocol multiplexer
	mux := protocol.NewProtocolMux(b)
	registerProtocols(mux, cfg, b)

	// Start multiplexer on main port
	go func() {
		if err := mux.Serve(); err != nil {
			fmt.Fprintf(os.Stderr, "Mux error: %v\n", err)
		}
	}()

	// Start admin HTTP server on separate port
	adminServer := httpadmin.NewAdminServer(b)

	// Register WebSocket handler on admin server if enabled
	if cfg.Protocols.WebSocket.Enabled {
		wsServer := ws.NewServer(b)
		adminServer.RegisterWebSocket(cfg.Protocols.WebSocket.Path, wsServer)
	}

	go func() {
		if err := adminServer.Serve(); err != nil {
			fmt.Fprintf(os.Stderr, "Admin server error: %v\n", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")

	// Graceful shutdown
	mux.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	adminServer.Shutdown(ctx)
	b.Stop()
	fmt.Println("Goodbye.")
}

// registerProtocols registers all enabled protocol adapters with the multiplexer.
func registerProtocols(mux *protocol.ProtocolMux, cfg *broker.Config, b *broker.Broker) {
	// Register in detection order: AMQP → MQTT → HTTP → Chimera

	if cfg.Protocols.AMQP.Enabled {
		amqpServer := amqp.NewServer(b)
		mux.Register(&amqp.Detector{}, amqpServer)
		b.Logger().Info("AMQP 1.0 adapter enabled")
	}

	if cfg.Protocols.MQTT.Enabled {
		mqttServer := mqtt.NewServer(b)
		mux.Register(&mqtt.Detector{}, mqttServer)
		b.Logger().Info("MQTT adapter enabled")
	}

	// HTTP is always registered on the main port for API access
	httpServer := httpadmin.NewAdminServer(b)
	mux.Register(&httpadmin.Detector{}, httpServer)

	if cfg.Protocols.Chimera.Enabled {
		chimeraServer, _ := chimera.NewServer(b)
		mux.Register(&chimera.Detector{}, chimeraServer)
		b.Logger().Info("Chimera native protocol enabled")
	}
}

// RunTopicCLI manages topics via the admin API.
func RunTopicCLI(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: chimera topic [create|list|describe|delete]")
		os.Exit(1)
	}

	adminAddr := getAdminAddr()

	switch args[0] {
	case "create":
		flags := flag.NewFlagSet("create", flag.ExitOnError)
		name := flags.String("name", "", "Topic name")
		mode := flags.String("mode", "unified", "Topic mode: stream, queue, unified")
		partitions := flags.Uint("partitions", 8, "Number of partitions")
		flags.Parse(args[1:])

		body, _ := json.Marshal(map[string]interface{}{
			"name":       *name,
			"mode":       *mode,
			"partitions": *partitions,
		})

		resp, err := httpPost(adminAddr+"/v1/topics", body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printResponse(resp)

	case "list":
		resp, err := httpGet(adminAddr + "/v1/topics")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printResponse(resp)

	case "describe":
		if len(args) < 2 {
			fmt.Println("Usage: chimera topic describe <name>")
			os.Exit(1)
		}
		resp, err := httpGet(adminAddr + "/v1/topics/" + args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printResponse(resp)

	case "delete":
		if len(args) < 2 {
			fmt.Println("Usage: chimera topic delete <name>")
			os.Exit(1)
		}
		resp, err := httpDelete(adminAddr + "/v1/topics/" + args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		printResponse(resp)
	}
}

// RunProduceCLI produces messages via the admin API.
func RunProduceCLI(args []string) {
	flags := flag.NewFlagSet("produce", flag.ExitOnError)
	topic := flags.String("topic", "", "Target topic")
	msg := flags.String("message", "", "Message body (or stdin)")
	count := flags.Int("count", 1, "Number of messages")
	flags.Parse(args)

	adminAddr := getAdminAddr()

	var body []byte
	if *msg != "" {
		body = []byte(*msg)
	} else {
		fmt.Println("Reading from stdin...")
		body = readStdin()
	}

	for i := 0; i < *count; i++ {
		resp, err := httpPost(adminAddr+"/v1/messages/"+*topic, body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		fmt.Printf("Published: partition=%v offset=%v\n", result["partition"], result["offset"])
	}
}

// RunConsumeCLI consumes messages via the admin API.
func RunConsumeCLI(args []string) {
	flags := flag.NewFlagSet("consume", flag.ExitOnError)
	topic := flags.String("topic", "", "Source topic")
	partition := flags.Uint("partition", 0, "Partition ID")
	offset := flags.Uint64("offset", 0, "Start offset")
	limit := flags.Int("limit", 10, "Max messages to fetch")
	follow := flags.Bool("follow", false, "Keep polling for new messages")
	flags.Parse(args)

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "Error: --topic is required")
		os.Exit(1)
	}

	adminAddr := getAdminAddr()

	for {
		url := fmt.Sprintf("%s/v1/messages/%s?partition=%d&offset=%d&limit=%d&timeout=2s",
			adminAddr, *topic, *partition, *offset, *limit)

		resp, err := httpGet(url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			if !*follow {
				os.Exit(1)
			}
			time.Sleep(1 * time.Second)
			continue
		}

		var result struct {
			Messages []struct {
				Offset  uint64 `json:"offset"`
				Payload []byte `json:"payload"`
			} `json:"messages"`
			NextOffset uint64 `json:"next_offset"`
			Count      int    `json:"count"`
		}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		for _, msg := range result.Messages {
			fmt.Printf("offset=%d payload=%s\n", msg.Offset, string(msg.Payload))
		}

		if result.Count > 0 {
			*offset = result.NextOffset
		}

		if !*follow {
			break
		}
	}
}
