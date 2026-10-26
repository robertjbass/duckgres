package server

import (
	"strings"
	"testing"
)

func TestRedactSecretsRemovesObjectStorageAccessKeyIDs(t *testing.T) {
	id := "AKIA" + strings.Repeat("Q", 16)
	r2 := strings.Repeat("a1", 16)
	cases := []string{
		"HTTP 403 Forbidden\n\nInvalidAccessKeyId: The AWS Access Key Id you provided does not exist in our records.\nInvalid Access Key: \"" + r2 + "\" Failure",
		"<Error><AWSAccessKeyId>" + r2 + "</AWSAccessKeyId></Error>",
		"credentials for " + id + " were rejected",
	}
	for _, input := range cases {
		got := RedactSecrets(input)
		if strings.Contains(got, id) || strings.Contains(got, r2) {
			t.Fatalf("access key id survived redaction: %q", got)
		}
		if !strings.Contains(got, "[REDACTED]") {
			t.Fatalf("no redaction marker in %q", got)
		}
	}
	plain := "Catalog Error: Table with name real_local does not exist!"
	if got := RedactSecrets(plain); got != plain {
		t.Fatalf("unrelated text changed: %q", got)
	}
}
