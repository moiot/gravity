package collector_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	. "github.com/moiot/gravity/dcp/collector"
	"github.com/moiot/gravity/pkg/protocol/dcp"
)

var _ = Describe("Grpc Collector", func() {

	config := GrpcConfig{
		Port: 8888,
	}

	XIt("should serve client request", func() {
		server := NewGrpc(&config)
		server.Start()

		msg := &dcp.Message{Id: "1"}
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", config.Port), grpc.WithInsecure())
		Expect(err).ShouldNot(HaveOccurred())
		defer conn.Close()

		client := dcp.NewDCPServiceClient(conn)
		stream, err := client.Process(context.Background())
		Expect(err).ShouldNot(HaveOccurred())

		err = stream.Send(msg)
		Expect(err).ShouldNot(HaveOccurred())
		Eventually(server.GetChan()).Should(Receive(Equal(msg)))

		resp, err := stream.Recv()
		Expect(err).ShouldNot(HaveOccurred())

		Expect(resp.Id).Should(Equal(msg.Id))
		Expect(resp.Code).Should(Equal(int32(0)))

		go func() {
			for {
				err = stream.Send(msg)
				Expect(err).ShouldNot(HaveOccurred())

				time.Sleep(time.Millisecond * 5)
			}
		}()

		server.Stop()
		Eventually(server.GetChan()).Should(BeClosed())
	})
})
