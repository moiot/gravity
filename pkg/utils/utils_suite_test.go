package utils_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/logutil"
)

func TestUtils(t *testing.T) {
	logutil.MustInitLogger(&logutil.LogConfig{Format: "text"})
	log.SetOutput(GinkgoWriter)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Utils Suite")
}
