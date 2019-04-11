package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenIndex(t *testing.T) {
	r := require.New(t)

	r.Equal("orders", genIndexName("ORDERS"))
	r.Equal("user_info", genIndexName("_user_info"))
}
