package job_processor_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestJobProcessor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pad job worker Suite")
}
