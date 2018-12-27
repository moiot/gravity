package main

import (
	"flag"
	"os"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"encoding/json"
	"io/ioutil"

	"github.com/moiot/gravity/padder"
	"github.com/moiot/gravity/padder/config"
	"github.com/moiot/gravity/pkg/logutil"
	"github.com/moiot/gravity/pkg/utils"
)

// main is the bootstrap.
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

	if cfg.ConfigFile == "" {
		log.Fatalf("config file is required")
	}

	if err := cfg.CreateConfigFromFile(cfg.ConfigFile); err != nil {
		log.Fatalf("failed to load config from file. %v", err)
	}
	if err = config.Validate(cfg.PadderConfig); err != nil {
		log.Fatalf("config validation failed: %v", err)
	}
	logutil.MustInitLogger(&cfg.Log)
	utils.LogRawInfo("padder")
	if cfg.PreviewMode {
		stats, err := padder.Preview(cfg.PadderConfig)
		if err != nil {
			log.Fatalf("pad preview bin log failed: %v", err)
		}
		statsJson, err := json.MarshalIndent(stats, "", "    ")
		if err != nil {
			log.Fatalf("parse json failed: %v", err)
		}
		err = ioutil.WriteFile("stats.json", statsJson, 0644)
		if err != nil {
			log.Fatalf("export preview statistic failed: %v", err)
		}
	} else {
		if err := padder.Pad(cfg.PadderConfig); err != nil {
			log.Fatalf("pad bin log failed: %v", err)
		}
	}
}
