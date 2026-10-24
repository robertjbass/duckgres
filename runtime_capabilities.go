package main

// Compiled support only: callers must also qualify the artifact/extension and
// prove authenticated listener readiness before exposing a connect endpoint.
func runtimeCapabilitiesJSON() string {
	return `{"schema_version":1,"features":{"external_postgres_v1":true,"quack_v1":true}}`
}
