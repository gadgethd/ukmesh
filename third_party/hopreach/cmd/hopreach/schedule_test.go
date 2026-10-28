package main

import (
	"path/filepath"
	"testing"
	"time"

	"hopreach/internal/analytics"
)

func scheduleTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}

func TestRecomputeDue(t *testing.T) {
	now := scheduleTime("2026-10-28T01:30:00Z")
	for _, tt := range []struct {
		name      string
		scheduled bool
		cron      string
		last      string
		force     bool
		want      bool
	}{
		{"manual run before tonight's slot", true, "0 1 * * *", "2026-10-27T23:32:00Z", false, true},
		{"boot run before slot on same day", true, "0 1 * * *", "2026-10-28T00:59:00Z", false, true},
		{"completed after slot", true, "0 1 * * *", "2026-10-28T01:10:00Z", false, false},
		{"completed exactly at slot", true, "0 1 * * *", "2026-10-28T01:00:00Z", false, false},
		{"no history", true, "0 1 * * *", "", false, true},
		{"bad cron uses interval and skips recent run", true, "not cron", "2026-10-28T00:59:00Z", false, false},
		{"complex cron uses interval and runs when old", true, "0 1 * * 0", "2026-10-27T01:10:00Z", false, true},
		{"normal startup keeps interval guard", false, "0 1 * * *", "2026-10-27T23:32:00Z", false, false},
		{"normal startup runs when old", false, "0 1 * * *", "2026-10-27T01:10:00Z", false, true},
		{"force bypasses normal guard", false, "0 1 * * *", "2026-10-28T01:10:00Z", true, true},
		{"force bypasses scheduled guard", true, "0 1 * * *", "2026-10-28T01:10:00Z", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := appConfig{scheduled: tt.scheduled, scheduleCron: tt.cron, minRecomputeIntervalHours: 6, forceRecompute: tt.force}
			var last time.Time
			if tt.last != "" {
				last = scheduleTime(tt.last)
			}
			if got := recomputeDue(cfg, last, tt.last != "", now); got != tt.want {
				t.Fatalf("recomputeDue = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMostRecentScheduledSlot(t *testing.T) {
	for _, tt := range []struct{ cron, now, want string }{
		{"0 1 * * *", "2026-10-28T01:00:00Z", "2026-10-28T01:00:00Z"},
		{"0 1 * * *", "2026-10-28T00:30:00Z", "2026-10-27T01:00:00Z"},
		{"15 1 * * *", "2026-10-28T01:10:00Z", "2026-10-27T01:15:00Z"},
		{"0 0 * * *", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"},
		{" 05\t01 * * * ", "2026-10-28T01:10:00+02:00", "2026-10-28T01:05:00+02:00"},
	} {
		got, ok := mostRecentScheduledSlot(tt.cron, scheduleTime(tt.now))
		if !ok || !got.Equal(scheduleTime(tt.want)) {
			t.Errorf("slot(%q, %s) = %s, %v; want %s", tt.cron, tt.now, got, ok, tt.want)
		}
	}
	for _, cron := range []string{"", "nonsense", "* * * * *", "0 1 * * 0", "0 1 1 * *", "0 1 * 10 *", "*/15 1 * * *", "60 1 * * *", "0 24 * * *", "-1 1 * * *", "+1 1 * * *", "0 1 * * * *"} {
		if _, ok := mostRecentScheduledSlot(cron, time.Now()); ok {
			t.Errorf("%q should use the interval fallback", cron)
		}
	}
}

func TestTierNeedsCompute(t *testing.T) {
	now := scheduleTime("2026-10-28T01:30:00Z")
	for _, tt := range []struct {
		name, generated     string
		scheduled, forceAll bool
		want                bool
	}{
		{"scheduled pre-slot boot tier", "2026-10-28T00:59:00Z", true, false, true},
		{"scheduled post-slot tier", "2026-10-28T01:10:00Z", true, false, false},
		{"scheduled exact-slot tier", "2026-10-28T01:00:00Z", true, false, false},
		{"scheduled missing timestamp", "", true, false, true},
		{"scheduled invalid timestamp", "invalid", true, false, true},
		{"normal force retains same-day tier", "2026-10-28T00:59:00Z", false, false, false},
		{"normal force recomputes previous day", "2026-10-27T23:32:00Z", false, false, true},
		{"force-all overrides scheduled freshness", "2026-10-28T01:10:00Z", true, true, true},
		{"force-all overrides normal freshness", "2026-10-28T01:10:00Z", false, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := appConfig{scheduled: tt.scheduled, scheduleCron: "0 1 * * *", forceRecompute: true, forceAllTiers: tt.forceAll}
			if got := tierNeedsCompute(cfg, &coverageMeta{GeneratedAt: tt.generated}, now); got != tt.want {
				t.Errorf("tierNeedsCompute = %v, want %v", got, tt.want)
			}
		})
	}
	if !tierNeedsCompute(appConfig{scheduled: true, scheduleCron: "0 1 * * *"}, nil, now) {
		t.Error("a missing tier must be computed")
	}
	if tierNeedsCompute(appConfig{scheduled: true, scheduleCron: "bad cron"}, &coverageMeta{GeneratedAt: "2026-10-28T00:59:00Z"}, now) {
		t.Error("unsupported cron must retain the same-day tier fallback")
	}
}

func TestLastCompletedAt(t *testing.T) {
	generated := "2026-10-28T00:30:00Z"
	finished := "2026-10-28T01:10:00Z"
	for _, tt := range []struct {
		name, completed string
		complete, want  bool
	}{
		{"completed across slot", finished, true, true},
		{"incomplete run cannot suppress schedule", finished, false, false},
		{"invalid completion cannot suppress schedule", "invalid", true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := writeJSONFile(filepath.Join(dir, "meta.json"), meta{GeneratedAt: generated, CompletedAt: tt.completed, Complete: tt.complete}); err != nil {
				t.Fatal(err)
			}
			got, ok := lastCompletedAt(dir)
			if ok != tt.want || (ok && !got.Equal(scheduleTime(finished))) {
				t.Fatalf("lastCompletedAt = %s, %v", got, ok)
			}
			if old, ok := lastGeneratedAt(dir); tt.complete && (!ok || !old.Equal(scheduleTime(generated))) {
				t.Fatal("normal-run generation-time semantics changed")
			}
		})
	}
	if _, ok := lastCompletedAt(t.TempDir()); ok {
		t.Error("no history must not suppress the schedule")
	}
}

func TestLegacyCompletionUsesSuccessfulRunHistory(t *testing.T) {
	dir := t.TempDir()
	generated := scheduleTime("2026-10-28T00:30:00Z")
	finished := scheduleTime("2026-10-28T01:10:00Z")
	if err := writeJSONFile(filepath.Join(dir, "meta.json"), meta{GeneratedAt: generated.Format(time.RFC3339), Complete: true}); err != nil {
		t.Fatal(err)
	}
	if got, ok := lastCompletedAt(dir); !ok || !got.Equal(generated) {
		t.Fatal("legacy metadata without history should conservatively use generated_at")
	}
	for _, record := range []analytics.RunRecord{
		{StartedAt: generated.Add(-time.Minute), FinishedAt: finished, Success: true},
		{StartedAt: finished, FinishedAt: finished.Add(time.Minute), Success: false},
	} {
		if err := analytics.Append(analyticsPath(dir, "runs.jsonl"), record, 10); err != nil {
			t.Fatal(err)
		}
	}
	if got, ok := lastCompletedAt(dir); !ok || !got.Equal(finished) {
		t.Fatalf("legacy completion = %s, %v; want %s", got, ok, finished)
	}
}

func TestScheduledRunSkipsCompletedRunAcrossSlot(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	if err := writeJSONFile(filepath.Join(dir, "meta.json"), meta{
		GeneratedAt: now.Add(-24 * time.Hour).Format(time.RFC3339),
		CompletedAt: now.Format(time.RFC3339), Complete: true,
	}); err != nil {
		t.Fatal(err)
	}
	// A skip must return before contacting the source or starting computation.
	cfg := appConfig{scheduled: true, scheduleCron: "0 0 * * *", outputDir: dir, demCacheDir: t.TempDir(), apiURL: "invalid", minRecomputeIntervalHours: 6}
	if err := run(cfg); err != nil {
		t.Fatalf("completed scheduled run should have skipped: %v", err)
	}
}
