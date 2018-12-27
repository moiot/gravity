package checker_test

import (
	"testing"

	"net/http"
	_ "net/http/pprof"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/logutil"
)

func TestChecker(t *testing.T) {
	logutil.SetLogLevelFromEnv()
	log.SetOutput(GinkgoWriter)

	go http.ListenAndServe("0.0.0.0:8000", nil)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Checker Suite")
}
