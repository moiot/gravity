package sql_execution_engine

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSQLExecutionEngine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sql execution engine Suite")
}
