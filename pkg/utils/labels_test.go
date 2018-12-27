package utils_test

import (
	. "github.com/moiot/gravity/pkg/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("test labels", func() {
	It("returns the right labels", func() {
		envString := "a=v1,b=v2"
		labels, err := GetLabelsFromEnv(envString)
		Expect(err).To(BeNil())
		Expect(labels["a"]).To(Equal("v1"))
		Expect(labels["b"]).To(Equal("v2"))

		envString = ""
		labels, err = GetLabelsFromEnv(envString)
		Expect(err).To(BeNil())

		envString = "a=v1"
		labels, err = GetLabelsFromEnv(envString)
		Expect(err).To(BeNil())
		Expect(labels["a"]).To(Equal("v1"))
	})
})
