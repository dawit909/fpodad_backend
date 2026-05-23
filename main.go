package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

// The JSON structure the Android app will send (The Write Path)
type Submission struct {
    EpisodeID      string `json:"episode_id"`
    Fingerprint    string `json:"fingerprint"`
    SkipDurationMs int    `json:"skip_duration_ms"`
}

// Represents a single, distinct ad skip (Used inside the response)
type AdSkip struct {
    Fingerprint    string `json:"fingerprint"`
    SkipDurationMs int    `json:"skip_duration_ms"`
}

// The JSON structure the server will return to the app (The Read Path)
type SkipResponse struct {
    EpisodeID    string   `json:"episode_id"`
    Fingerprints []AdSkip `json:"fingerprints"` 
}

func main() {
	var err error
	// Opens (or creates) a local SQLite database file
	db, err = sql.Open("sqlite3", "./podskips.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Initialize the database table
	initDB()

	// Set up our two API routes
	http.HandleFunc("/api/submit", submitFingerprintHandler)
	http.HandleFunc("/api/skips", getSkipsHandler)
	
	log.Println("Go Server running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func initDB() {
	// Create the table. We track the fingerprint, the episode, and a trust score.
	// We use a UNIQUE constraint on (episode_id, fingerprint) to prevent duplicates.
	query := `
	CREATE TABLE IF NOT EXISTS ad_fingerprints (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		episode_id TEXT NOT NULL,
		fingerprint TEXT NOT NULL,
		trust_score INTEGER DEFAULT 1,
		UNIQUE(episode_id, fingerprint)
	);`

	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
}

func submitFingerprintHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var sub Submission
	err := json.NewDecoder(r.Body).Decode(&sub)
	if err != nil || sub.EpisodeID == "" || sub.Fingerprint == "" {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	// Insert the new fingerprint. If it already exists, increment the trust_score by 1.
	// This is SQLite's version of an "Upsert".
	query := `
	INSERT INTO ad_fingerprints (episode_id, fingerprint, trust_score) 
	VALUES (?, ?, 1)
	ON CONFLICT(episode_id, fingerprint) 
	DO UPDATE SET trust_score = trust_score + 1;`

	_, err = db.Exec(query, sub.EpisodeID, sub.Fingerprint)
	if err != nil {
		log.Printf("DB Insert Error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status":"success"}`))
}

func getSkipsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	episodeID := r.URL.Query().Get("episode_id")
	if episodeID == "" {
		http.Error(w, "Missing episode_id", http.StatusBadRequest)
		return
	}

	// We only return fingerprints that have a trust score of 1 or higher.
	// As your app grows, you can bump this to 2 or 3 to require multiple verifications.
	query := `
	SELECT fingerprint FROM ad_fingerprints 
	WHERE episode_id = ? AND trust_score >= 1`

	rows, err := db.Query(query, episodeID)
	if err != nil {
		log.Printf("DB Query Error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var fingerprints []string
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err == nil {
			fingerprints = append(fingerprints, fp)
		}
	}

	// Send the JSON response back to the Android app
	response := SkipResponse{
		EpisodeID:    episodeID,
		Fingerprints: fingerprints,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
