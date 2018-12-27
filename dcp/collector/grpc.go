package collector

import (
	"io"

	"github.com/moiot/gravity/pkg/protocol/dcp"

	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Grpc struct {
	port   int
	server *grpc.Server
	output chan *dcp.Message
}

func NewGrpc(config *GrpcConfig) Interface {
	s := &Grpc{
		port:   config.Port,
		server: grpc.NewServer(),
		output: make(chan *dcp.Message, 100),
	}
	dcp.RegisterDCPServiceServer(s.server, s)
	return s
}

func (s *Grpc) Start() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go s.server.Serve(lis)
}

func (s *Grpc) Stop() {
	s.server.Stop()
	close(s.output)
	log.Info("collector.Grpc stopped")
}

func (s *Grpc) GetChan() chan *dcp.Message {
	return s.output
}

func (s *Grpc) Process(stream dcp.DCPService_ProcessServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Info("collector.Grpc receive EOF")
			return nil
		}

		if err != nil {
			log.Error("collector.Grpc ", err)
			return err
		}

		s.output <- msg

		err = stream.Send(&dcp.Response{
			Id: msg.Id,
		})

		if err != nil {
			log.Info("collector.Grpc send error ", err)
			return err
		}
	}
}
