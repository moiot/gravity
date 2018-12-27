package collector_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/logutil"
)

func TestCollector(t *testing.T) {
	logutil.SetLogLevelFromEnv()
	log.SetOutput(GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Collector Suite")
}
