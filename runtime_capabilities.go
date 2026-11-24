package main

// Compiled support only. The stock Layerbase runtime advertises only the
// features it carries; account-runtime connectors are not part of it.
//
// copy_cancel_v1: a COPY ... TO file stops on a CancelRequest, a client
// disconnect or pg_terminate_backend from another session of the same user.
//
// file_lockdown_v1: a file-persistence database loads only DuckDB-signed
// extensions, installs and loads none after startup, and reads or writes
// files only in its exchange directory (/tmp/duckgres-exchange/<data dir
// name>), its temp, extension and secret directories, and remote URLs. The
// control plane stages imports and collects exports in the exchange
// directory and relies on excel being preloaded.
func runtimeCapabilitiesJSON() string {
	return `{"schema_version":1,"features":{"copy_cancel_v1":true,"file_lockdown_v1":true}}`
}
