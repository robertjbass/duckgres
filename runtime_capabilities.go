package main

// This descriptor proves compiled configuration support, not extension/runtime
// readiness. Callers must verify the executable's qualified artifact digest too.
func runtimeCapabilitiesJSON() string {
	return `{"schema_version":1,"features":{"external_postgres_v1":true,"quack_v1":false}}`
}
