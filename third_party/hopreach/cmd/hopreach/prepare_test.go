package main

import (
	"testing"

	yconfig "hopreach/internal/config"
)

func TestPrepareCronUsesScheduledMode(t *testing.T) {
	yc := yconfig.Config{}
	yc.Schedule.Cron = "0 1 * * *"
	want := "HOPREACH_CONFIG=/config/config.yaml\n" +
		"0 1 * * * root /app/hopreach -scheduled >> /var/log/fetch.log 2>&1\n" +
		"17 4 * * * root /app/hopreach-shareapi -prune >> /var/log/prune.log 2>&1\n"
	if got := renderCronFile(yc, "/config/config.yaml"); got != want {
		t.Fatalf("cron file = %q, want %q", got, want)
	}
	if cfg := toAppConfig(yc); cfg.scheduleCron != yc.Schedule.Cron || cfg.scheduled {
		t.Fatalf("cron must reach the due check without opting ordinary runs into scheduled mode: %+v", cfg)
	}
}
