package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"hopreach/internal/analytics"
)

// The clock's location matches cron's local timezone (UTC in the container).
// Complex expressions deliberately retain the existing minimum-interval rule.
func mostRecentScheduledSlot(cron string, now time.Time) (time.Time, bool) {
	fields := strings.Fields(cron)
	if len(fields) != 5 || fields[2] != "*" || fields[3] != "*" || fields[4] != "*" {
		return time.Time{}, false
	}
	for _, digit := range fields[0] + fields[1] {
		if digit < '0' || digit > '9' {
			return time.Time{}, false
		}
	}
	minute, minuteErr := strconv.Atoi(fields[0])
	hour, hourErr := strconv.Atoi(fields[1])
	if minuteErr != nil || hourErr != nil || minute > 59 || hour > 23 {
		return time.Time{}, false
	}
	slot := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, now.Location())
	if slot.After(now) {
		slot = slot.AddDate(0, 0, -1)
	}
	return slot, true
}

func recomputeDue(cfg appConfig, last time.Time, hasLast bool, now time.Time) bool {
	if cfg.forceRecompute || !hasLast {
		return true
	}
	if cfg.scheduled {
		if slot, ok := mostRecentScheduledSlot(cfg.scheduleCron, now); ok {
			return last.Before(slot)
		}
	}
	return now.Sub(last) >= time.Duration(cfg.minRecomputeIntervalHours*float64(time.Hour))
}

func tierNeedsCompute(cfg appConfig, previous *coverageMeta, now time.Time) bool {
	if cfg.forceAllTiers {
		return true
	}
	if cfg.scheduled {
		if slot, ok := mostRecentScheduledSlot(cfg.scheduleCron, now); ok {
			if previous == nil {
				return true
			}
			finished, err := time.Parse(time.RFC3339, previous.GeneratedAt)
			return err != nil || finished.Before(slot)
		}
	}
	return !tierFreshToday(previous, now)
}

// generated_at is written before computing, so it cannot identify a run that
// crossed the cron slot. New metadata records completion explicitly; legacy
// metadata can recover the finish time from its successful analytics record.
func lastCompletedAt(outputDir string) (time.Time, bool) {
	generated, ok := lastGeneratedAt(outputDir)
	if !ok {
		return time.Time{}, false
	}
	data, err := os.ReadFile(filepath.Join(outputDir, "meta.json"))
	if err != nil {
		return time.Time{}, false
	}
	var previous struct {
		CompletedAt string `json:"completed_at"`
	}
	if err := json.Unmarshal(data, &previous); err != nil {
		return time.Time{}, false
	}
	if previous.CompletedAt != "" {
		finished, err := time.Parse(time.RFC3339, previous.CompletedAt)
		return finished, err == nil
	}
	records, _ := analytics.ReadAll[analytics.RunRecord](analyticsPath(outputDir, "runs.jsonl"))
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Success && !record.FinishedAt.Before(generated) && record.StartedAt.Before(generated.Add(time.Second)) {
			return record.FinishedAt, true
		}
	}
	// Without a completion record, a complete dataset generated after the
	// slot is still sufficient proof; an older one conservatively runs again.
	return generated, true
}
