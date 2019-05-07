package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/fsnotify/fsnotify"
	hplugin "github.com/hashicorp/go-plugin"
	jsoniter "github.com/json-iterator/go"
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

	runtime.SetBlockProfileRate(cfg.BlockProfileRate)
	runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)

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

	log.RegisterExitHandler(func() {
		hplugin.CleanupClients()
	})

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

	// When deploying with k8s cluster,
	// batch mode won't continue if the return code of this process is 0.
	// This process exit with 0 only when the input is done in batch mode.
	// For all the other cases, this process exit with 1.
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
			os.Exit(1)

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
			os.Exit(1)

		case err, ok := <-watcher.Errors:
			if !ok {
				continue
			}
			log.Println("error:", err)
			server.Close()
			os.Exit(1)
		}
	}
}

func healthzHandler(server *app.Server) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if server.Scheduler.Healthy() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func statusHandler(server *app.Server, name, hash string) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		position, v, exist, err := server.PositionCache.GetEncodedPersistentPosition()
		if err != nil || !exist {
			writer.WriteHeader(http.StatusInternalServerError)
			log.Errorf("[statusHandler] failed to get positionRepoModel, exist: %v, err: %v", exist, errors.ErrorStack(err))
			return
		}

		var state = core.ReportStageIncremental
		if position.Stage == config.Batch {
			state = core.ReportStageFull
		}

		ret := core.TaskReportStatus{
			Name:       name,
			ConfigHash: hash,
			Position:   v,
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

func resetHandler(server *app.Server, _ config.PipelineConfigV3) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		if err := server.PositionCache.Clear(); err != nil {
			log.Errorf("[reset] failed to clear position, err: %v", errors.ErrorStack(err))
			http.Error(writer, fmt.Sprintf("failed to clear position, err: %v", errors.ErrorStack(err)), 500)
			return
		}
		server.Close()
		os.Exit(1)
	}
}
