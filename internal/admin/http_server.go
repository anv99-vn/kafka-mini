package admin

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/vna/kafka-mini/internal/broker"
)

type HttpServer struct {
	manager *broker.TopicManager
}

func NewHttpServer(manager *broker.TopicManager) *HttpServer {
	return &HttpServer{manager: manager}
}

func (s *HttpServer) Start(addr string) error {
	mux := http.NewServeMux()

	// Static files
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	// API endpoints
	mux.HandleFunc("/api/topics", s.handleTopics)
	mux.HandleFunc("/api/topics/", s.handleTopicDetail) // For DELETE

	return http.ListenAndServe(addr, mux)
}

func (s *HttpServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		topics := s.manager.ListTopics()
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
		s.manager.CreateTopic(req.Name, req.Partitions)
		w.WriteHeader(http.StatusCreated)

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
	s.manager.DeleteTopic(name)
	w.WriteHeader(http.StatusNoContent)
}
