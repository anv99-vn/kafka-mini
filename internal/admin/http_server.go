package admin

import (
	"encoding/json"
	"net/http"
	"strings"

	"context"
	pb "github.com/vna/kafka-mini/proto"
	"github.com/vna/kafka-mini/internal/broker"
)

type HttpServer struct {
	b *broker.Broker
}

func NewHttpServer(b *broker.Broker) *HttpServer {
	return &HttpServer{b: b}
}

func (s *HttpServer) Start(addr string) error {
	mux := http.NewServeMux()

	// Static files
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	// API endpoints
	mux.HandleFunc("/api/topics", s.handleTopics)
	mux.HandleFunc("/api/topics/", s.handleTopicDetail) // For DELETE
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/groups", s.handleGroups)

	return http.ListenAndServe(addr, mux)
}

func (s *HttpServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		topics := s.b.Manager.ListTopics()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(topics)

	case http.MethodPost:
		var req struct {
			Name              string `json:"name"`
			Partitions        int32  `json:"partitions"`
			ReplicationFactor int32  `json:"replication_factor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res, _ := s.b.CreateTopic(context.Background(), &pb.CreateTopicRequest{
			Name:       req.Name,
			Partitions: req.Partitions,
		})
		
		if !res.Success {
			http.Error(w, res.ErrorMessage, http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HttpServer) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	name := parts[3]
	res, err := s.b.DeleteTopic(context.Background(), &pb.DeleteTopicRequest{Name: name})
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_ = res
	w.WriteHeader(http.StatusNoContent)
}

func (s *HttpServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	id, state, term, leaderId := s.b.Raft.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":   id,
		"state":     state,
		"term":      term,
		"leader_id": leaderId,
	})
}

func (s *HttpServer) handleGroups(w http.ResponseWriter, r *http.Request) {
	groups := s.b.ListGroups()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(groups)
}
