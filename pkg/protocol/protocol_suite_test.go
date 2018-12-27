package protocol_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestRouterConfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "protocol Suite")
}
