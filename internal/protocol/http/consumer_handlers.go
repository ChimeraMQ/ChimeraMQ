package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/chimeramq/chimera/internal/engine/stream"
)

func (s *AdminServer) handleConsumerJoin(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	if group == "" || len(group) > 255 {
		writeError(w, http.StatusBadRequest, "invalid group name")
		return
	}
	var req struct {
		MemberID   string `json:"member_id"`
		Topic      string `json:"topic"`
		Partitions uint32 `json:"partitions"`
		Strategy   string `json:"strategy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.MemberID == "" || req.Topic == "" {
		writeError(w, http.StatusBadRequest, "member_id and topic are required")
		return
	}
	if len(req.MemberID) > 255 {
		writeError(w, http.StatusBadRequest, "member_id too long")
		return
	}
	if req.Partitions == 0 {
		req.Partitions = 1
	}

	strategy := stream.StrategyRange
	if req.Strategy == "round_robin" {
		strategy = stream.StrategyRoundRobin
	}

	s.broker.StreamEngine().JoinGroup(group, req.Topic, req.MemberID, req.Partitions, strategy)

	cg := s.broker.StreamEngine().GetGroup(group)
	assignments := map[string][]uint32{}
	if cg != nil {
		for part, memberID := range cg.Assignments() {
			assignments[memberID] = append(assignments[memberID], part)
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"group":       group,
		"member_id":   req.MemberID,
		"assignments": assignments,
	})
}

func (s *AdminServer) handleConsumerLeave(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	var req struct {
		MemberID string `json:"member_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.MemberID == "" {
		writeError(w, http.StatusBadRequest, "member_id is required")
		return
	}

	s.broker.StreamEngine().LeaveGroup(group, req.MemberID)
	writeJSON(w, http.StatusOK, map[string]string{
		"status":    "left",
		"group":     group,
		"member_id": req.MemberID,
	})
}

func (s *AdminServer) handleConsumerHeartbeat(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	var req struct {
		MemberID string `json:"member_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.MemberID == "" {
		writeError(w, http.StatusBadRequest, "member_id is required")
		return
	}

	if err := s.broker.StreamEngine().Heartbeat(group, req.MemberID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *AdminServer) handleConsumerOffsets(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	cg := s.broker.StreamEngine().GetGroup(group)
	if cg == nil {
		writeError(w, http.StatusNotFound, "consumer group not found")
		return
	}

	offsets := map[string]uint64{}
	assignments := cg.Assignments()
	for part := range assignments {
		offsets[fmt.Sprintf("%d", part)] = cg.GetCommittedOffset(part)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"group":   group,
		"offsets": offsets,
	})
}

func (s *AdminServer) handleConsumerCommitOffsets(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	var req struct {
		Offsets map[string]uint64 `json:"offsets"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	committed := 0
	for partStr, offset := range req.Offsets {
		partID, err := strconv.ParseUint(partStr, 10, 32)
		if err != nil {
			continue
		}
		if err := s.broker.StreamEngine().CommitOffset(group, uint32(partID), offset); err != nil {
			continue
		}
		committed++
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"committed": committed,
		"total":     len(req.Offsets),
	})
}
