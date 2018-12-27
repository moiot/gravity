package integration_test

import (
	"net/http"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	go http.ListenAndServe("0.0.0.0:8000", nil)

	os.Exit(m.Run())
}
