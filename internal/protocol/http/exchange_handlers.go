package http

import (
	"net/http"

	"github.com/chimeramq/chimera/internal/engine/exchange"
	"github.com/chimeramq/chimera/internal/message"
)

func (s *AdminServer) handleCreateExchange(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Durable bool   `json:"durable"`
	}
	if err := decodeJSON(r, &req, 0); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" || len(req.Name) > 255 {
		writeError(w, http.StatusBadRequest, "invalid exchange name")
		return
	}

	xtype := exchange.ExchangeTypeFromString(req.Type)
	ex, err := s.broker.Exchanges().Declare(req.Name, xtype, req.Durable)
	if err != nil {
		writeErrorf(w, http.StatusConflict, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"name":    ex.Name(),
		"type":    ex.Type().String(),
		"durable": req.Durable,
	})
}

func (s *AdminServer) handleListExchanges(w http.ResponseWriter, r *http.Request) {
	names := s.broker.Exchanges().List()
	limit, offset := parsePagination(r)
	names = paginate(names, limit, offset)

	type exchangeInfo struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Bindings int    `json:"bindings"`
	}

	result := make([]exchangeInfo, 0, len(names))
	for _, name := range names {
		if ex, ok := s.broker.Exchanges().Get(name); ok {
			result = append(result, exchangeInfo{
				Name:     name,
				Type:     ex.Type().String(),
				Bindings: len(ex.Bindings()),
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"exchanges": result,
		"count":     len(result),
	})
}

func (s *AdminServer) handleGetExchange(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ex, ok := s.broker.Exchanges().Get(name)
	if !ok {
		writeError(w, http.StatusNotFound, "exchange not found")
		return
	}

	bindings := ex.Bindings()
	type bindingInfo struct {
		Key         string            `json:"key"`
		Destination string            `json:"destination"`
		Arguments   map[string]string `json:"arguments,omitempty"`
	}

	bindingList := make([]bindingInfo, 0, len(bindings))
	for _, b := range bindings {
		bindingList = append(bindingList, bindingInfo{
			Key:         b.Key,
			Destination: b.Destination,
			Arguments:   b.Arguments,
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":     name,
		"type":     ex.Type().String(),
		"bindings": bindingList,
	})
}

func (s *AdminServer) handleDeleteExchange(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.broker.Exchanges().Delete(name); err != nil {
		writeErrorf(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *AdminServer) handleBindExchange(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		Key         string            `json:"key"`
		Destination string            `json:"destination"`
		Arguments   map[string]string `json:"arguments,omitempty"`
	}
	if err := decodeJSON(r, &req, 0); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Destination == "" {
		writeError(w, http.StatusBadRequest, "destination is required")
		return
	}

	if err := s.broker.Exchanges().Bind(name, req.Key, req.Destination, req.Arguments); err != nil {
		writeErrorf(w, http.StatusNotFound, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "bound"})
}

func (s *AdminServer) handleUnbindExchange(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		Key         string `json:"key"`
		Destination string `json:"destination"`
	}
	if err := decodeJSON(r, &req, 0); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := s.broker.Exchanges().Unbind(name, req.Key, req.Destination); err != nil {
		writeErrorf(w, http.StatusNotFound, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "unbound"})
}

func (s *AdminServer) handlePublishToExchange(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		RoutingKey string            `json:"routing_key"`
		Headers    map[string]string `json:"headers,omitempty"`
		Payload    []byte            `json:"payload"`
	}
	if err := decodeJSON(r, &req, 0); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	destinations, err := s.broker.Exchanges().Route(name, req.RoutingKey, req.Headers)
	if err != nil {
		writeErrorf(w, http.StatusNotFound, err)
		return
	}

	published := 0
	for _, dest := range destinations {
		env := &message.Envelope{
			Topic:       dest,
			RoutingKey:  req.RoutingKey,
			Payload:     req.Payload,
			SourceProto: message.ProtoHTTP,
		}
		if _, err := s.broker.Publish(env); err == nil {
			published++
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"routed":    len(destinations),
		"published": published,
	})
}
