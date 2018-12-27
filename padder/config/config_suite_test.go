package config_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestPadder(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Padder Config Suite")
}
