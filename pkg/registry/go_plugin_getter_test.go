package registry

import (
	"os"
	"runtime"
	"testing"

	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"
)

func TestGoPluginGetter(t *testing.T) {
	assert := assert.New(t)

	defer func() {
		os.Remove(PluginDir)
	}()

	var fileUrl string
	if runtime.GOOS == "darwin" {
		fileUrl = "./test_data/dump_filter_plugin.darwin.so"
	} else if runtime.GOOS == "linux" {
		fileUrl = "./test_data/dump_filter_plugin.linux.so"
	}

	configData := map[string]interface{}{
		"name": "dump_filter_plugin",
		"url":  fileUrl,
	}

	_, _, err := DownloadGoNativePlugin(configData)
	if err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}

}
