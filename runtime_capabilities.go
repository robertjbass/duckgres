package main

// Compiled support only. The stock Layerbase runtime advertises only the
// features it carries; account-runtime connectors are not part of it.
//
// copy_cancel_v1: a COPY ... TO file stops on a CancelRequest, a client
// disconnect or pg_terminate_backend from another session of the same user.
func runtimeCapabilitiesJSON() string {
	return `{"schema_version":1,"features":{"copy_cancel_v1":true}}`
}
