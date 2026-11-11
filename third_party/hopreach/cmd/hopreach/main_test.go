package main

import (
	"flag"
	"io"
	"reflect"
	"testing"
)

func TestCommandFlags(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want commandFlags
	}{
		{"default unchanged", nil, commandFlags{}},
		{"cron mode", []string{"-scheduled"}, commandFlags{scheduled: true}},
		{"explicit false", []string{"-scheduled=false"}, commandFlags{}},
		{"force unchanged", []string{"-force"}, commandFlags{force: true}},
		{"force-all unchanged", []string{"-force-all-tiers"}, commandFlags{forceAllTiers: true}},
		{"scheduled explicit force", []string{"-scheduled", "-force", "-config", "/config/config.yaml"}, commandFlags{scheduled: true, force: true, configPath: "/config/config.yaml"}},
		{"prepare unchanged", []string{"-prepare"}, commandFlags{prepare: true}},
		{"node unchanged", []string{"-node", "test-key"}, commandFlags{node: "test-key"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet("hopreach", flag.ContinueOnError)
			opts := registerFlags(fs)
			if err := fs.Parse(tt.args); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*opts, tt.want) {
				t.Errorf("flags = %+v; want %+v", *opts, tt.want)
			}
		})
	}
	fs := flag.NewFlagSet("hopreach", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registerFlags(fs)
	if err := fs.Parse([]string{"-scheduled=invalid"}); err == nil {
		t.Error("invalid scheduled flag must fail parsing")
	}
}
