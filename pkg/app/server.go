package app

import (
	"sync"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/emitter"
	"github.com/moiot/gravity/pkg/filters"
	_ "github.com/moiot/gravity/pkg/inputs"
	_ "github.com/moiot/gravity/pkg/outputs"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schedulers/batch_table_scheduler"
)

type Server struct {
	Input         core.Input
	filters       []core.IFilter
	Emitter       core.Emitter
	Scheduler     core.Scheduler
	PositionCache position_store.PositionCacheInterface
	Output        core.Output

	// When Input is done, server will be closed, when config changed, server will also be closed;
	// add a lock here to prevent race condition
	isClosed bool
	sync.Mutex
}

func Parse(pipelineConfig config.PipelineConfigV3) (*Server, error) {
	pipelineConfig = pipelineConfig.DeepCopy()

	server := Server{}

	// output
	plugin, err := registry.GetPlugin(registry.OutputPlugin, pipelineConfig.OutputPlugin.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// type assertion
	output, ok := plugin.(core.Output)
	if !ok {
		return nil, errors.Errorf("not a valid output plugin: %v", pipelineConfig.OutputPlugin.Type)
	}
	server.Output = output

	if err := plugin.Configure(pipelineConfig.PipelineName, pipelineConfig.OutputPlugin.Config); err != nil {
		return nil, errors.Trace(err)
	}

	// scheduler
	if pipelineConfig.SchedulerPlugin == nil {
		pipelineConfig.SchedulerPlugin = &config.GenericConfig{
			Type:   "batch-table-scheduler",
			Config: batch_table_scheduler.DefaultConfig,
		}
	}

	plugin, err = registry.GetPlugin(registry.SchedulerPlugin, pipelineConfig.SchedulerPlugin.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}

	scheduler, ok := plugin.(core.Scheduler)
	if !ok {
		return nil, errors.Errorf("not a valid scheduler plugin")
	}
	server.Scheduler = scheduler

	if err := plugin.Configure(pipelineConfig.PipelineName, pipelineConfig.SchedulerPlugin.Config); err != nil {
		return nil, errors.Trace(err)
	}

	// emitters
	fs, err := filters.NewFilters(pipelineConfig.FilterPlugins)
	if err != nil {
		return nil, errors.Trace(err)
	}
	server.filters = fs

	e, err := emitter.NewEmitter(fs, server.Scheduler)
	if err != nil {
		return nil, errors.Trace(err)
	}
	server.Emitter = e

	// input
	inputPlugins := pipelineConfig.InputPlugin
	input, err := newInput(pipelineConfig.PipelineName, inputPlugins)
	if err != nil {
		return nil, errors.Trace(err)
	}
	server.Input = input

	return &server, nil
}

func NewServer(pipelineConfig config.PipelineConfigV3) (*Server, error) {
	server, err := Parse(pipelineConfig)
	if err != nil {
		return nil, err
	}

	// position store
	newer, ok := server.Input.(core.PositionCacheCreator)
	if !ok {
		return nil, errors.Errorf("input plugin is not a position cache creator")
	}

	newer.NewPositionCache()

	if p, err := newer.NewPositionCache(); err != nil {
		return nil, errors.Trace(err)
	} else {
		server.PositionCache = p
	}
	return server, nil
}

func newInput(pipelineName string, inputConfig config.InputConfig) (core.Input, error) {
	plugin, err := registry.GetPlugin(registry.InputPlugin, inputConfig.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}

	inputConfig.Config["mode"] = inputConfig.Mode
	if err := plugin.Configure(pipelineName, inputConfig.Config); err != nil {
		return nil, errors.Trace(err)
	}

	p, ok := plugin.(core.Input)
	if !ok {
		return nil, errors.Errorf("not a valid input type")
	} else {
		return p, nil
	}
}

func (s *Server) Start() error {
	s.Lock()
	defer s.Unlock()

	switch o := s.Output.(type) {
	case core.SynchronousOutput:
		if err := o.Start(); err != nil {
			return errors.Trace(err)
		}
	case core.AsynchronousOutput:
		if err := o.Start(s.Scheduler); err != nil {
			return errors.Trace(err)
		}
	default:
		return errors.Errorf("output is an invalid type")
	}

	if err := s.Scheduler.Start(s.Output); err != nil {
		return errors.Trace(err)
	}

	if err := s.PositionCache.Start(); err != nil {
		return errors.Trace(err)
	}

	log.Infof("[Server] start input")

	if err := s.Input.Start(s.Emitter, s.Output.GetRouter(), s.PositionCache); err != nil {
		return errors.Trace(err)
	}

	s.isClosed = false

	log.Infof("[Server] started")
	return nil
}

func (s *Server) Close() {
	s.Lock()
	defer s.Unlock()

	if s.isClosed {
		return
	}

	log.Infof("[Server] closing..")
	s.Input.Close()
	log.Infof("[Server] input closed")

	s.Scheduler.Close()
	log.Infof("[Server] scheduler closed")

	s.Output.Close()
	log.Infof("[Server] output closed")

	s.PositionCache.Close()
	log.Infof("[Server] position store closed")

	log.Infof("[Server] stopped")

	s.isClosed = true
}
