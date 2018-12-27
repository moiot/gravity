package collector

import "github.com/moiot/gravity/pkg/protocol/dcp"

type Interface interface {
	GetChan() chan *dcp.Message
	Start()
	Stop()
}
