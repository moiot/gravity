package sliding_window_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSlidingWindow(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sliding Window")
}
