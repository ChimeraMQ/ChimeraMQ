package mqtt

import (
	"strings"
	"sync"
)

// TopicMapper handles MQTT topic to ChimeraMQ topic conversion.
type TopicMapper struct {
	separator string // ChimeraMQ separator (default ".")
}

// NewTopicMapper creates a new topic mapper.
func NewTopicMapper(separator string) *TopicMapper {
	if separator == "" {
		separator = "."
	}
	return &TopicMapper{separator: separator}
}

// MQTTToChimera converts an MQTT topic to a ChimeraMQ topic.
// MQTT uses '/' as separator; ChimeraMQ uses the configured separator.
// e.g., "sensor/temperature/room1" → "sensor.temperature.room1"
func (tm *TopicMapper) MQTTToChimera(mqttTopic string) string {
	return strings.ReplaceAll(mqttTopic, "/", tm.separator)
}

// ChimeraToMQTT converts a ChimeraMQ topic back to MQTT format.
func (tm *TopicMapper) ChimeraToMQTT(chimeraTopic string) string {
	return strings.ReplaceAll(chimeraTopic, tm.separator, "/")
}

// FilterMatches checks if an MQTT topic filter matches a topic name.
// Supports:
//   - '+' single-level wildcard
//   - '#' multi-level wildcard (must be last segment)
func FilterMatches(filter, topic string) bool {
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	fi := 0
	ti := 0

	for fi < len(filterParts) {
		if ti >= len(topicParts) {
			// Remaining filter parts must all be '#'
			for ; fi < len(filterParts); fi++ {
				if filterParts[fi] != "#" {
					return false
				}
			}
			return true
		}

		switch filterParts[fi] {
		case "#":
			return true // matches everything remaining
		case "+":
			// matches any single level — advance both
			fi++
			ti++
		default:
			if filterParts[fi] != topicParts[ti] {
				return false
			}
			fi++
			ti++
		}
	}

	return ti == len(topicParts)
}

// RetainedStore holds retained messages in memory.
type RetainedStore struct {
	mu       sync.RWMutex
	messages map[string]*RetainedMessage
	max      int
}

// RetainedMessage is a message stored for new subscribers.
type RetainedMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
}

// NewRetainedStore creates a retained message store.
func NewRetainedStore(max int) *RetainedStore {
	if max <= 0 {
		max = 10000
	}
	return &RetainedStore{
		messages: make(map[string]*RetainedMessage),
		max:      max,
	}
}

// Store saves a retained message. If payload is empty, removes the entry.
func (rs *RetainedStore) Store(topic string, payload []byte, qos byte) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(payload) == 0 {
		delete(rs.messages, topic)
		return
	}

	if len(rs.messages) >= rs.max {
		// Evict oldest (arbitrary — could be LRU but keep simple for now)
		for k := range rs.messages {
			delete(rs.messages, k)
			break
		}
	}

	rs.messages[topic] = &RetainedMessage{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
	}
}

// Matching returns retained messages matching an MQTT topic filter.
func (rs *RetainedStore) Matching(filter string) []*RetainedMessage {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var result []*RetainedMessage
	for _, msg := range rs.messages {
		if FilterMatches(filter, msg.Topic) {
			result = append(result, msg)
		}
	}
	return result
}
