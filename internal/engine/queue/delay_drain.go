package queue

// drainDelayQueue reads promoted messages from the delay scheduler
// and dispatches them to available consumers.
func (e *Engine) drainDelayQueue(topic string, qs *QueueState) {
	ch := qs.delayHeap.Ready()
	for env := range ch {
		if env == nil {
			continue
		}
		// Reset delay fields so it's treated as a normal message
		env.DeliverAt = 0

		// Try to dispatch to a consumer
		qs.mu.Lock()
		if len(qs.consumers) == 0 {
			qs.mu.Unlock()
			// Re-schedule: no consumers available yet
			env.DeliverAt = 1 // past time, will re-promote on next tick
			qs.delayHeap.Schedule(env)
			continue
		}
		offset := env.Sequence
		consumerID, err := qs.dispatcher.Dispatch(offset, env)
		if err != nil {
			qs.mu.Unlock()
			continue
		}
		qs.ackTracker.Track(offset, consumerID, env.DeliverCount, env.MaxRetries)
		qs.mu.Unlock()
	}
}
