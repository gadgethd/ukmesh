package sysinfo

import "testing"

func TestFreeDiskBytes(t *testing.T) {
	bytes, err := FreeDiskBytes(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if bytes == 0 {
		t.Fatal("expected non-zero free disk")
	}
}
