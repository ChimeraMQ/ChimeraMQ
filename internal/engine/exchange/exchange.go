package exchange

import (
	"strings"
	"sync"
)

// ExchangeType determines how messages are routed to bound destinations.
type ExchangeType uint8

const (
	TypeDirect  ExchangeType = 0
	TypeTopic   ExchangeType = 1
	TypeFanout  ExchangeType = 2
	TypeHeaders ExchangeType = 3
)

// Binding represents a rule that links a routing pattern to a destination topic.
type Binding struct {
	Key         string            // routing key (exact for direct, pattern for topic)
	Destination string            // target topic name
	Arguments   map[string]string // for headers exchange matching
}

// Exchange routes messages to bound destinations based on type and routing key.
type Exchange struct {
	mu       sync.RWMutex
	name     string
	xtype    ExchangeType
	bindings []Binding
	durable  bool
}

// NewExchange creates a new exchange.
func NewExchange(name string, xtype ExchangeType, durable bool) *Exchange {
	return &Exchange{
		name:    name,
		xtype:   xtype,
		durable: durable,
	}
}

// Name returns the exchange name.
func (e *Exchange) Name() string { return e.name }

// Type returns the exchange type.
func (e *Exchange) Type() ExchangeType { return e.xtype }

// Bind adds a binding to the exchange.
func (e *Exchange) Bind(key, destination string, args map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bindings = append(e.bindings, Binding{
		Key:         key,
		Destination: destination,
		Arguments:   args,
	})
}

// Unbind removes a binding by key and destination.
func (e *Exchange) Unbind(key, destination string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, b := range e.bindings {
		if b.Key == key && b.Destination == destination {
			e.bindings = append(e.bindings[:i], e.bindings[i+1:]...)
			return
		}
	}
}

// Bindings returns all bindings.
func (e *Exchange) Bindings() []Binding {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]Binding, len(e.bindings))
	copy(result, e.bindings)
	return result
}

// Route returns the list of destination topics for a given routing key and headers.
func (e *Exchange) Route(routingKey string, headers map[string]string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	switch e.xtype {
	case TypeDirect:
		return e.routeDirect(routingKey)
	case TypeTopic:
		return e.routeTopic(routingKey)
	case TypeFanout:
		return e.routeFanout()
	case TypeHeaders:
		return e.routeHeaders(headers)
	default:
		return nil
	}
}

func (e *Exchange) routeDirect(key string) []string {
	var dests []string
	for _, b := range e.bindings {
		if b.Key == key {
			dests = append(dests, b.Destination)
		}
	}
	return dests
}

func (e *Exchange) routeTopic(key string) []string {
	var dests []string
	for _, b := range e.bindings {
		if topicMatch(b.Key, key) {
			dests = append(dests, b.Destination)
		}
	}
	return dests
}

func (e *Exchange) routeFanout() []string {
	dests := make([]string, 0, len(e.bindings))
	for _, b := range e.bindings {
		dests = append(dests, b.Destination)
	}
	return dests
}

func (e *Exchange) routeHeaders(headers map[string]string) []string {
	var dests []string
	for _, b := range e.bindings {
		if headersMatch(b.Arguments, headers) {
			dests = append(dests, b.Destination)
		}
	}
	return dests
}

// topicMatch checks if a routing key matches a topic pattern.
// Patterns support '*' (single word) and '#' (zero or more words).
// Words are separated by '.'.
func topicMatch(pattern, key string) bool {
	if pattern == "#" {
		return true
	}
	if pattern == key {
		return true
	}

	patternParts := strings.Split(pattern, ".")
	keyParts := strings.Split(key, ".")

	return matchParts(patternParts, keyParts, 0, 0)
}

func matchParts(pattern, key []string, pi, ki int) bool {
	for pi < len(pattern) && ki < len(key) {
		switch pattern[pi] {
		case "#":
			// '#' matches zero or more words
			// Try matching rest of pattern at every position from ki onward
			for k := ki; k <= len(key); k++ {
				if matchParts(pattern, key, pi+1, k) {
					return true
				}
			}
			return false
		case "*":
			// '*' matches exactly one word
			pi++
			ki++
		default:
			if pattern[pi] != key[ki] {
				return false
			}
			pi++
			ki++
		}
	}

	// Consume trailing '#' patterns
	for pi < len(pattern) && pattern[pi] == "#" {
		pi++
	}

	return pi == len(pattern) && ki == len(key)
}

// headersMatch checks if binding arguments match the message headers.
// For a binding to match, all binding arguments must be present in the
// message headers with matching values. If the binding has an "x-match"
// argument set to "any", at least one header must match.
func headersMatch(bindingArgs, msgHeaders map[string]string) bool {
	if len(bindingArgs) == 0 {
		return true
	}
	if len(msgHeaders) == 0 {
		return false
	}

	matchMode := "all"
	if v, ok := bindingArgs["x-match"]; ok {
		matchMode = v
	}

	switch matchMode {
	case "any":
		for k, v := range bindingArgs {
			if k == "x-match" {
				continue
			}
			if mv, ok := msgHeaders[k]; ok && mv == v {
				return true
			}
		}
		return false
	default: // "all"
		for k, v := range bindingArgs {
			if k == "x-match" {
				continue
			}
			if mv, ok := msgHeaders[k]; !ok || mv != v {
				return false
			}
		}
		return true
	}
}

// ExchangeTypeFromString parses an exchange type string.
func ExchangeTypeFromString(s string) ExchangeType {
	switch strings.ToLower(s) {
	case "direct":
		return TypeDirect
	case "topic":
		return TypeTopic
	case "fanout":
		return TypeFanout
	case "headers":
		return TypeHeaders
	default:
		return TypeDirect
	}
}

// String returns the exchange type name.
func (t ExchangeType) String() string {
	switch t {
	case TypeDirect:
		return "direct"
	case TypeTopic:
		return "topic"
	case TypeFanout:
		return "fanout"
	case TypeHeaders:
		return "headers"
	default:
		return "unknown"
	}
}
