package main

import (
	"encoding/json"
	"net/http"
)

type SkipSegment struct {
	StartMs int    `json:"start_ms"`
	EndMs   int    `json:"end_ms"`
	Type    string `json:"type"`
}

type SkipResponse struct {
	EpisodeID    string        `json:"episode_id"`
	SkipSegments []SkipSegment `json:"skip_segments"`
}

func skipHandler(w http.ResponseWriter, r *http.Request) {
	// In production, grab the episode ID from the URL query
	// episodeID := r.URL.Query().Get("episode_id")

	response := SkipResponse{
		EpisodeID: "test_episode_123",
		SkipSegments: []SkipSegment{
			{StartMs: 5000, EndMs: 55000, Type: "sponsor"}, // Skip an ad from 5s to 55s
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	http.HandleFunc("/api/skips", skipHandler)
	http.ListenAndServe(":8080", nil) // Run locally on port 8080
}
