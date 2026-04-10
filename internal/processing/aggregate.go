package processing

// AggregateFunc takes current state and a new event, returns new state and
// whether to emit the result.
type AggregateFunc func(state []byte, event []byte) (newState []byte, emit bool)

// AggregateOp combines windowing, state, and an aggregation function.
type AggregateOp struct {
	windowMgr *WindowManager
	state     *StateStore
	aggFunc   AggregateFunc
}

// NewAggregateOp creates a new aggregate operator.
func NewAggregateOp(wm *WindowManager, store *StateStore, fn AggregateFunc) *AggregateOp {
	return &AggregateOp{
		windowMgr: wm,
		state:     store,
		aggFunc:   fn,
	}
}

// Process adds an event to the window and runs aggregation.
func (a *AggregateOp) Process(key string, timestamp int64, data []byte) {
	a.windowMgr.AddEvent(key, timestamp, data)
}

// Tick advances the window and emits aggregates.
func (a *AggregateOp) Tick(now int64) {
	a.windowMgr.Tick(now)
}

// Close shuts down the aggregate operator.
func (a *AggregateOp) Close() {
	a.windowMgr.Close()
}
