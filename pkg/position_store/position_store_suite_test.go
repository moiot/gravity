package position_store_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGravity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PositionStore Suite")
}
