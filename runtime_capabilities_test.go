package main

import (
	"encoding/json"
	"testing"
)

func TestRuntimeCapabilities(t *testing.T) {
	var got struct {
		SchemaVersion int             `json:"schema_version"`
		Features      map[string]bool `json:"features"`
	}
	if err := json.Unmarshal([]byte(runtimeCapabilitiesJSON()), &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || !got.Features["external_postgres_v1"] || !got.Features["quack_v1"] {
		t.Fatalf("unexpected capabilities: %+v", got)
	}
}
