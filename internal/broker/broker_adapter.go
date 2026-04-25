package broker

import (
	"strconv"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/processing"
)

// Ensure brokerAPIAdapter implements processing.BrokerAPI.
var _ processing.BrokerAPI = (*brokerAPIAdapter)(nil)

// brokerAPIAdapter implements processing.BrokerAPI by delegating to the Broker.
type brokerAPIAdapter struct {
	broker *Broker
}

func (a *brokerAPIAdapter) FetchMessages(topic string, partition uint32, offset uint64, limit int) ([]*message.Envelope, error) {
	part, err := a.broker.storage.GetOrCreatePartition(topic, partition)
	if err != nil {
		return nil, err
	}
	data, err := part.ReadRange(offset, ^uint64(0), limit)
	if err != nil {
		return nil, err
	}

	// Decrypt if at-rest encryption is enabled
	decryptor := a.broker.encryptor
	segmentID := topic + "/" + strconv.Itoa(int(partition))

	envs := make([]*message.Envelope, 0, len(data))
	for _, d := range data {
		if decryptor != nil {
			decrypted, err := decryptor.Decrypt(d, segmentID)
			if err != nil {
				continue // skip unreadable entries
			}
			d = decrypted
		}
		env, err := message.Unmarshal(d)
		if err != nil {
			continue
		}
		envs = append(envs, env)
	}
	return envs, nil
}

func (a *brokerAPIAdapter) PublishMessage(topic string, env *message.Envelope) (uint64, uint32, error) {
	env.Topic = topic
	offset, err := a.broker.Publish(env)
	if err != nil {
		return 0, env.PartitionID, err
	}
	return offset, env.PartitionID, nil
}

func (a *brokerAPIAdapter) TopicPartitions(topic string) int {
	tc, ok := a.broker.topics.GetTopic(topic)
	if !ok {
		return 0
	}
	return int(tc.Partitions)
}
