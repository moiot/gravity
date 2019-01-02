package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/moiot/gravity/gravity/inputs/stages"

	"github.com/fsnotify/fsnotify"
	"github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/gravity"
	"github.com/moiot/gravity/gravity/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/logutil"
	"github.com/moiot/gravity/pkg/utils"
)

var myJson = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

func main() {
	cfg := config.NewConfig()
	err := cfg.ParseCmd(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("parse cmd flags errors: %s\n", err)
	}

	if cfg.Version {
		utils.PrintRawInfo("gravity")
		os.Exit(0)
	}

	if cfg.ConfigFile != "" {
		if err := cfg.ConfigFromFile(cfg.ConfigFile); err != nil {
			log.Fatalf("failed to load config from file: %v", errors.ErrorStack(err))
		}
	} else {
		log.Fatal("config must not be empty")
	}

	content, err := ioutil.ReadFile(cfg.ConfigFile)
	if err != nil {
		log.Fatalf("fail to read file %s. err: %s", cfg.ConfigFile, err)
	}
	hash := core.HashConfig(string(content))

	logutil.MustInitLogger(&cfg.Log)
	utils.LogRawInfo("gravity")

	pipelineConfig := cfg.PipelineConfig
	if err := config.ValidatePipelineConfig(pipelineConfig); err != nil {
		log.Fatalf("[gravity] pipeline config validation failed: %v", errors.ErrorStack(err))
	}

	logutil.PipelineName = pipelineConfig.PipelineName

	server, err := gravity.NewServer(pipelineConfig)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.ClearPosition {
		positionStore := server.Input.PositionStore()
		pos := positionStore.Position()
		positionStore.Clear()
		log.Infof("position cleared: %s", pos)
		return
	}

	err = server.Start()
	if err != nil {
		log.Fatal(err)
	}

	lock := &sync.Mutex{}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/reset", resetHandler(server, lock, pipelineConfig))
		http.HandleFunc("/status", statusHandler(server, pipelineConfig.PipelineName, hash))
		http.HandleFunc("/healthz", healthzHandler(server))
		err = http.ListenAndServe(cfg.HttpAddr, nil)
		if err != nil {
			log.Fatalf("http error: %v", err)
		}
		log.Info("starting http server")
	}()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(errors.Trace(err))
	}
	defer watcher.Close()

	err = watcher.Add(cfg.ConfigFile)
	if err != nil {
		log.Fatal(errors.Trace(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	for {
		select {
		case sig := <-sc:
			log.Infof("[gravity] stop with signal %v", sig)
			lock.Lock()
			server.Close()
			lock.Unlock()
			return

		case event, ok := <-watcher.Events:
			if !ok {
				continue
			}
			if event.Name != cfg.ConfigFile {
				continue
			}

			log.Info("config file event: ", event.String())

			content, err := ioutil.ReadFile(cfg.ConfigFile)
			if err != nil {
				log.Infof("read config error: %s", err)
			} else {
				newHash := core.HashConfig(string(content))
				if newHash == hash {
					log.Infof("config not changed")
					continue
				}
			}

			log.Info("config file updated, quit...")
			lock.Lock()
			server.Close()
			lock.Unlock()
			return

		case err, ok := <-watcher.Errors:
			if !ok {
				continue
			}
			log.Println("error:", err)
			lock.Lock()
			server.Close()
			lock.Unlock()
		}
	}
}

func healthzHandler(server *gravity.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if server.Scheduler.Healthy() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func statusHandler(server *gravity.Server, name, config string) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		pos, err := myJson.MarshalToString(server.Input.PositionStore().Position().Raw)
		if err != nil {
			log.Error(err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		var state = core.ReportStageIncremental
		if server.Input.Stage() == stages.InputStageFull {
			state = core.ReportStageFull
		}

		ret := core.TaskReportStatus{
			Name:       name,
			ConfigHash: config,
			Position:   pos,
			Stage:      state,
			Version:    utils.Version,
		}

		resp, err := myJson.Marshal(ret)
		if err != nil {
			log.Error(err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(resp)
	}
}

func resetHandler(server *gravity.Server, lock *sync.Mutex, pipelineConfig *config.PipelineConfigV2) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		lock.Lock()
		defer lock.Unlock()

		server.Close()

		server, err := gravity.NewServer(pipelineConfig)
		if err != nil {
			log.Errorf("[reset] fail to new server, err: %s", err)
			http.Error(writer, fmt.Sprintf("fail to new server, err: %s", err), 500)
			return
		}
		server.Input.PositionStore().Clear()

		server, err = gravity.NewServer(pipelineConfig)
		if err != nil {
			log.Errorf("[reset] fail to new server, err: %s", err)
			http.Error(writer, fmt.Sprintf("fail to new server, err: %s", err), 500)
			return
		}
		if err := server.Start(); err != nil {
			log.Errorf("[reset] fail to start server, err: %s", err)
			http.Error(writer, fmt.Sprintf("fail to start server, err: %s", err), 500)
			return
		}
	}
}
