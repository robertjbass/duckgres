package main

// Compiled support only: callers must also qualify the artifact/extension and
// prove authenticated listener readiness before exposing a connect endpoint.
//
// copy_cancel_v1: a COPY ... TO file stops on a CancelRequest, a client
// disconnect or pg_terminate_backend from another session of the same user.
func runtimeCapabilitiesJSON() string {
	return `{"schema_version":1,"features":{"external_postgres_v1":true,"quack_v1":true,"copy_cancel_v1":true}}`
}
