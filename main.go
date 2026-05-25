package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/time/rate"
)

// Rate Limiter tracking map
var visitors = make(map[string]*rate.Limiter)
var mtx sync.Mutex

func getVisitor(ip string) *rate.Limiter {
	mtx.Lock()
	defer mtx.Unlock()

	limiter, exists := visitors[ip]
	if !exists {
		// Allow 2 requests per second, with a burst capacity of 5
		limiter = rate.NewLimiter(2, 5)
		visitors[ip] = limiter
	}
	return limiter
}

func rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract IP address (naive approach, update if behind a proxy like Nginx)
		ip := r.RemoteAddr
		limiter := getVisitor(ip)

		if !limiter.Allow() {
			http.Error(w, "Too many requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

var db *sql.DB

type Submission struct {
	ClientID       string `json:"client_id"`
	EpisodeID      string `json:"episode_id"`
	Fingerprint    string `json:"fingerprint"`
	TimestampMs    int    `json:"timestamp_ms"` // NEW: Absolute start time
	SkipDurationMs int    `json:"skip_duration_ms"`
}

type AdSkip struct {
	TimestampMs    int    `json:"timestamp_ms"` // NEW: Returned to client
	SkipDurationMs int    `json:"skip_duration_ms"`
	Fingerprint    string `json:"fingerprint"`
}

type SkipResponse struct {
	EpisodeID    string   `json:"episode_id"`
	Fingerprints []AdSkip `json:"fingerprints"`
}

type ReportPayload struct {
	ClientID    string `json:"client_id"`
	EpisodeID   string `json:"episode_id"`
	TimestampMs int    `json:"timestamp_ms"`
}

func main() {
	var err error
	// 1. Enable WAL mode and set a 5-second busy timeout so requests wait in line instead of failing
	db, err = sql.Open("sqlite3", "./podskips.db?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 2. Configure the connection pool
	db.SetMaxOpenConns(25)                 // Max concurrent connections
	db.SetMaxIdleConns(5)                  // Keep 5 connections alive for fast response
	db.SetConnMaxLifetime(5 * time.Minute) // Cycle connections safely

	initDB()

	// Apply the middleware to your endpoints
	http.HandleFunc("/api/submit", rateLimitMiddleware(submitFingerprintHandler))
	http.HandleFunc("/api/report", rateLimitMiddleware(reportSkipHandler))
	http.HandleFunc("/api/skips", rateLimitMiddleware(getSkipsHandler))
	// Create a hardened server with timeouts
	server := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  5 * time.Second,  // Max time to read the request payload
		WriteTimeout: 10 * time.Second, // Max time to process and respond
		IdleTimeout:  15 * time.Second,
	}

	log.Println("Go Server running on http://localhost:8080")
	log.Fatal(server.ListenAndServe())
}

func initDB() {
	queryFingerprints := `
	CREATE TABLE IF NOT EXISTS ad_fingerprints (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		episode_id TEXT NOT NULL,
		timestamp_ms INTEGER NOT NULL,
		fingerprint TEXT NOT NULL,
		trust_score INTEGER DEFAULT 1,
		skip_duration_ms INTEGER NOT NULL
	);`

	queryVotes := `
	CREATE TABLE IF NOT EXISTS votes (
		client_id TEXT NOT NULL,
		episode_id TEXT NOT NULL,
		timestamp_ms INTEGER NOT NULL,
		PRIMARY KEY (client_id, episode_id, timestamp_ms)
	);`

	queryReports := `
	CREATE TABLE IF NOT EXISTS reports (
		client_id TEXT NOT NULL,
		episode_id TEXT NOT NULL,
		timestamp_ms INTEGER NOT NULL,
		PRIMARY KEY (client_id, episode_id, timestamp_ms)
	);`

	if _, err := db.Exec(queryFingerprints); err != nil {
		log.Fatalf("Failed to create ad_fingerprints table: %v", err)
	}
	if _, err := db.Exec(queryVotes); err != nil {
		log.Fatalf("Failed to create votes table: %v", err)
	}
	if _, err := db.Exec(queryReports); err != nil {
		log.Fatalf("Failed to create reports table: %v", err)
	}
}

func submitFingerprintHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var sub Submission
	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil || sub.ClientID == "" || sub.EpisodeID == "" || sub.TimestampMs < 0 {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	// 1. Time-based Clustering (Tolerance: 10 seconds)
	const timeToleranceMs = 10000
	rows, err := db.Query("SELECT id, timestamp_ms, skip_duration_ms FROM ad_fingerprints WHERE episode_id = ?", sub.EpisodeID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var matchedID int
	var existingDuration int
	var existingTimestamp int
	var shortestDistance = math.MaxInt32

	for rows.Next() {
		var dbID, dbTimestamp, dbDuration int
		if err := rows.Scan(&dbID, &dbTimestamp, &dbDuration); err == nil {
			dist := int(math.Abs(float64(sub.TimestampMs - dbTimestamp)))
			if dist < timeToleranceMs && dist < shortestDistance {
				shortestDistance = dist
				matchedID = dbID
				existingTimestamp = dbTimestamp
				existingDuration = dbDuration
			}
		}
	}

	// 2. Prevent Duplicate Votes on this specific cluster
	targetTimestamp := sub.TimestampMs
	if matchedID != 0 {
		targetTimestamp = existingTimestamp
	}

	voteQuery := `INSERT INTO votes (client_id, episode_id, timestamp_ms) VALUES (?, ?, ?)`
	if _, err = db.Exec(voteQuery, sub.ClientID, sub.EpisodeID, targetTimestamp); err != nil {
		log.Printf("Blocked duplicate vote from client: %s at %d", sub.ClientID, targetTimestamp)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ignored_duplicate"}`))
		return
	}

	// 3. Update Cluster or Insert New
	if matchedID != 0 {
		newDuration := (existingDuration + sub.SkipDurationMs) / 2
		updateQuery := `UPDATE ad_fingerprints SET trust_score = trust_score + 1, skip_duration_ms = ? WHERE id = ?`
		_, err = db.Exec(updateQuery, newDuration, matchedID)
		log.Printf("Clustered into ad ID %d (Time Diff: %dms)", matchedID, shortestDistance)
	} else {
		insertQuery := `INSERT INTO ad_fingerprints (episode_id, timestamp_ms, fingerprint, trust_score, skip_duration_ms) VALUES (?, ?, ?, 1, ?)`
		_, err = db.Exec(insertQuery, sub.EpisodeID, sub.TimestampMs, sub.Fingerprint, sub.SkipDurationMs)
		log.Printf("Created new ad cluster for episode %s at %dms", sub.EpisodeID, sub.TimestampMs)
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

	query := `
	SELECT timestamp_ms, skip_duration_ms, fingerprint FROM ad_fingerprints 
	WHERE episode_id = ? AND trust_score >= 3`

	rows, err := db.Query(query, episodeID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var skips []AdSkip
	for rows.Next() {
		var skip AdSkip
		if err := rows.Scan(&skip.TimestampMs, &skip.SkipDurationMs, &skip.Fingerprint); err == nil {
			skips = append(skips, skip)
		}
	}

	if skips == nil {
		skips = []AdSkip{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SkipResponse{
		EpisodeID:    episodeID,
		Fingerprints: skips,
	})
}

func reportSkipHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload ReportPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.ClientID == "" || payload.EpisodeID == "" {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	// 1. Prevent duplicate reports from the same device
	reportQuery := `INSERT INTO reports (client_id, episode_id, timestamp_ms) VALUES (?, ?, ?)`
	if _, err := db.Exec(reportQuery, payload.ClientID, payload.EpisodeID, payload.TimestampMs); err != nil {
		log.Printf("Blocked duplicate report from client: %s", payload.ClientID)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ignored_duplicate"}`))
		return
	}

	// 2. Decrement the trust score using temporal proximity (10-second tolerance)
	decrementQuery := `
	UPDATE ad_fingerprints 
	SET trust_score = trust_score - 1 
	WHERE episode_id = ? AND ABS(timestamp_ms - ?) < 10000`

	if _, err := db.Exec(decrementQuery, payload.EpisodeID, payload.TimestampMs); err != nil {
		log.Printf("DB Update Error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// 3. Purge clusters that fall to 0
	deleteQuery := `DELETE FROM ad_fingerprints WHERE trust_score <= 0`
	if _, err := db.Exec(deleteQuery); err != nil {
		log.Printf("DB Delete Error: %v", err)
	}

	log.Printf("Successfully processed report for episode %s at %dms", payload.EpisodeID, payload.TimestampMs)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}
