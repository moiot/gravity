package esmodel

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCapitalize(t *testing.T) {

	r := require.New(t)

	r.Equal("Capitalize", Capitalize("capitalize"))
}
