package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
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

	logutil.PipelineName = cfg.PipelineConfig.PipelineName

	server, err := app.NewServer(cfg.PipelineConfig)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	if cfg.ClearPosition {
		if err := server.PositionCache.Clear(); err != nil {
			log.Errorf("failed to clear position, err: %v", errors.ErrorStack(err))
		}
		return
	}

	err = server.Start()
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/reset", resetHandler(server, cfg.PipelineConfig))
		http.HandleFunc("/status", statusHandler(server, cfg.PipelineConfig.PipelineName, hash))
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

	if cfg.PipelineConfig.InputPlugin.Mode == config.Batch {
		go func(server *app.Server) {
			<-server.Input.Done()
			server.Close()
			os.Exit(0)
		}(server)
	}

	for {
		select {
		case sig := <-sc:
			log.Infof("[gravity] stop with signal %v", sig)
			server.Close()
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
			server.Close()
			return

		case err, ok := <-watcher.Errors:
			if !ok {
				continue
			}
			log.Println("error:", err)
			server.Close()
		}
	}
}

func healthzHandler(server *app.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if server.Scheduler.Healthy() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func statusHandler(server *app.Server, name, hash string) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		position, exist, err := server.PositionCache.Get()
		if err != nil || !exist {
			writer.WriteHeader(http.StatusInternalServerError)
			log.Error("[statusHandler] failed to get position, exist: %v, err: %v", exist, errors.ErrorStack(err))
			return
		}

		var state = core.ReportStageIncremental
		if position.Stage == config.Batch {
			state = core.ReportStageFull
		}

		ret := core.TaskReportStatus{
			Name:       name,
			ConfigHash: hash,
			Position:   position.Value,
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

func resetHandler(server *app.Server, pipelineConfig config.PipelineConfigV3) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		server.Close()

		server, err := app.NewServer(pipelineConfig)
		if err != nil {
			log.Errorf("[reset] fail to new server, err: %s", err)
			http.Error(writer, fmt.Sprintf("fail to new server, err: %s", err), 500)
			return
		}
		if err := server.PositionCache.Clear(); err != nil {
			log.Errorf("[reset] failed to clear position, err: %v", errors.ErrorStack(err))
			http.Error(writer, fmt.Sprintf("failed to clear position, err: %v", errors.ErrorStack(err)), 500)
			return
		}

		server, err = app.NewServer(pipelineConfig)
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
