package elasticsearch

import (
	"encoding/json"
	"fmt"
	"github.com/moiot/gravity/pkg/config"
	"testing"
)

func TestEsModelOutput_Configure(t *testing.T) {

	cfg := config.NewConfig()

	if err := cfg.ConfigFromFile("../../../bin/test.toml"); err != nil {
		println(err)
	}

	if bs, err := json.Marshal(cfg.PipelineConfig.OutputPlugin); err != nil {
		println(err)
	} else {
		fmt.Printf("%s\n", string(bs))
	}

}
