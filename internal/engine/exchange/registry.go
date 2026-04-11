package exchange

import (
	"fmt"
	"sync"
)

// Registry manages named exchanges.
type Registry struct {
	mu        sync.RWMutex
	exchanges map[string]*Exchange
}

// NewRegistry creates a new exchange registry.
func NewRegistry() *Registry {
	return &Registry{
		exchanges: make(map[string]*Exchange),
	}
}

// Declare creates a new exchange or returns an existing one.
// Returns an error if an exchange with the same name but different type exists.
func (r *Registry) Declare(name string, xtype ExchangeType, durable bool) (*Exchange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ex, ok := r.exchanges[name]; ok {
		if ex.Type() != xtype {
			return nil, fmt.Errorf("exchange %q already declared with type %s", name, ex.Type())
		}
		return ex, nil
	}

	ex := NewExchange(name, xtype, durable)
	r.exchanges[name] = ex
	return ex, nil
}

// Delete removes an exchange and all its bindings.
func (r *Registry) Delete(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.exchanges[name]; !ok {
		return fmt.Errorf("exchange %q not found", name)
	}
	delete(r.exchanges, name)
	return nil
}

// Get returns an exchange by name.
func (r *Registry) Get(name string) (*Exchange, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ex, ok := r.exchanges[name]
	return ex, ok
}

// List returns all exchange names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.exchanges))
	for name := range r.exchanges {
		names = append(names, name)
	}
	return names
}

// Route routes a message through the named exchange.
// Returns the list of destination topics or an error if the exchange doesn't exist.
func (r *Registry) Route(exchangeName, routingKey string, headers map[string]string) ([]string, error) {
	r.mu.RLock()
	ex, ok := r.exchanges[exchangeName]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("exchange %q not found", exchangeName)
	}
	return ex.Route(routingKey, headers), nil
}

// Bind adds a binding to an exchange.
func (r *Registry) Bind(exchangeName, key, destination string, args map[string]string) error {
	r.mu.RLock()
	ex, ok := r.exchanges[exchangeName]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("exchange %q not found", exchangeName)
	}
	ex.Bind(key, destination, args)
	return nil
}

// Unbind removes a binding from an exchange.
func (r *Registry) Unbind(exchangeName, key, destination string) error {
	r.mu.RLock()
	ex, ok := r.exchanges[exchangeName]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("exchange %q not found", exchangeName)
	}
	ex.Unbind(key, destination)
	return nil
}
