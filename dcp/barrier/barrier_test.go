package barrier_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"time"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/moiot/gravity/dcp/barrier"
)

var _ = Describe("Barrier", func() {
	It("should init meta table and periodically update offset", func() {
		db, mock, err := sqlmock.New()
		Expect(err).ShouldNot(HaveOccurred())
		defer db.Close()

		mock.ExpectExec("CREATE database if not EXISTS drc").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS drc.barrier").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("INSERT IGNORE").WillReturnResult(sqlmock.NewResult(1, 0))
		mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(1, 1))

		shutdown := make(chan struct{})

		barrier.Start(&barrier.Config{
			TestDB:        db,
			TickerSeconds: 1,
		}, shutdown)

		time.Sleep(time.Millisecond * 1500)

		close(shutdown)

		Expect(mock.ExpectationsWereMet()).Should(BeNil())
	})
})
