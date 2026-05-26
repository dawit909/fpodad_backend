package main

import (
	"log"
	"time"
)

// StartMaintenanceEngine runs the cleanup script automatically
func StartMaintenanceEngine() {
	ticker := time.NewTicker(24 * time.Hour) // Runs every 24 hours
	go func() {
		for range ticker.C {
			log.Println("Running database maintenance: Merging overlapping skips...")
			mergeOverlappingSkips()
		}
	}()
}

func mergeOverlappingSkips() {
	// 1. Find all episodes that have multiple ad skips
	rows, err := db.Query(`SELECT DISTINCT episode_id FROM ad_fingerprints`)
	if err != nil {
		log.Printf("Maintenance Error: %v", err)
		return
	}
	defer rows.Close()

	var episodes []string
	for rows.Next() {
		var ep string
		if err := rows.Scan(&ep); err == nil {
			episodes = append(episodes, ep)
		}
	}

	// 2. Process each episode to find and merge overlaps
	for _, ep := range episodes {
		processEpisodeClusters(ep)
		time.Sleep(50 * time.Millisecond)
	}
}

func processEpisodeClusters(episodeID string) {
	// Pull all skips for this episode, ordered by start time
	rows, err := db.Query(`
		SELECT id, timestamp_ms, skip_duration_ms, trust_score, fingerprint 
		FROM ad_fingerprints 
		WHERE episode_id = ? ORDER BY timestamp_ms ASC`, episodeID)
	if err != nil {
		return
	}
	defer rows.Close()

	var clusterIDs []int
	var totalWeight, weightedStart, weightedDuration int
	var lastEnd int
	var bestFingerprint string

	lastEnd = -2001

	for rows.Next() {
		var id, start, duration, trust int
		var fingerprint string
		rows.Scan(&id, &start, &duration, &trust, &fingerprint)

		// Check if this skip overlaps with the current cluster (+ 2 second tolerance)
		if len(clusterIDs) > 0 && start <= (lastEnd+2000) {
			// It overlaps! Add its mass to our running cluster math
			clusterIDs = append(clusterIDs, id)
			totalWeight += trust
			weightedStart += start * trust
			weightedDuration += duration * trust

			if start+duration > lastEnd {
				lastEnd = start + duration
			}
		} else {
			// No overlap. Resolve the previous cluster if it had multiple overlapping entries
			if len(clusterIDs) > 1 {
				finalizeCluster(clusterIDs, episodeID, bestFingerprint, weightedStart, weightedDuration, totalWeight)
			}
			// Start tracking a new potential cluster
			clusterIDs = []int{id}
			totalWeight = trust
			weightedStart = start * trust
			weightedDuration = duration * trust
			lastEnd = start + duration
			bestFingerprint = fingerprint
		}
	}

	// Resolve the final cluster if it was left hanging at the end of the loop
	if len(clusterIDs) > 1 {
		finalizeCluster(clusterIDs, episodeID, bestFingerprint, weightedStart, weightedDuration, totalWeight)
	}
}

func finalizeCluster(ids []int, episodeID, fingerprint string, weightedStart, weightedDuration, totalWeight int) {
	// NEW: Guard against fatal divide-by-zero panics
	if totalWeight <= 0 {
		totalWeight = 1
	}

	// Calculate the final consensus values
	consensusStart := weightedStart / totalWeight
	consensusDuration := weightedDuration / totalWeight

	// 1. Delete the messy overlapping rows
	for _, id := range ids {
		db.Exec(`DELETE FROM ad_fingerprints WHERE id = ?`, id)
	}

	// 2. Insert the clean, mathematically perfect row
	db.Exec(`INSERT INTO ad_fingerprints (episode_id, timestamp_ms, skip_duration_ms, trust_score, fingerprint) 
		VALUES (?, ?, ?, ?, ?)`, episodeID, consensusStart, consensusDuration, totalWeight, fingerprint)

	log.Printf("Merged %d overlapping skips into a single block at %dms for episode %s", len(ids), consensusStart, episodeID)
}
