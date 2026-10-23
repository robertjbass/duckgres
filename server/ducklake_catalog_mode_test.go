package server

import "testing"

func TestDuckLakeReplacesDefaultCatalog(t *testing.T) {
	if duckLakeReplacesDefaultCatalog(true) {
		t.Fatal("file persistence must keep the local catalog as the default")
	}
	if !duckLakeReplacesDefaultCatalog(false) {
		t.Fatal("memory mode keeps DuckLake as the default catalog")
	}
}
