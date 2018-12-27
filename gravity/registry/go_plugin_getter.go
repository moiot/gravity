package registry

import (
	"fmt"
	"os"
	"plugin"
	"reflect"

	"github.com/hashicorp/go-getter"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	PluginDir     = "./go-plugins"
	InterfaceName = "Plugin"
)

func DownloadGoPlugin(goPluginConfig map[string]interface{}) (string, Plugin, error) {
	pluginName, ok := goPluginConfig["name"]
	if !ok {
		return "", nil, errors.Errorf("go-plugin filter should have a unique name")
	}
	pluginNameString, ok := pluginName.(string)
	if !ok {
		return "", nil, errors.Errorf("filter name must be a string")
	}

	pluginUrl, ok := goPluginConfig["url"]
	if !ok {
		return "", nil, errors.Errorf("go-plugin filter should have a url to download")
	}
	pluginUrlString, ok := pluginUrl.(string)
	if !ok {
		return "", nil, errors.Errorf("filter url must be a string")
	}

	if _, err := os.Stat(PluginDir); os.IsNotExist(err) {
		if err := os.Mkdir(PluginDir, 755); err != nil {
			return "", nil, errors.Trace(err)
		}
	}

	dstFileName := fmt.Sprintf("%s/%s", PluginDir, pluginNameString)

	pwd, err := os.Getwd()
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	log.Infof("[registry] downloading plugin pwd %s, dstFileName: %s from %s", pwd, dstFileName, pluginUrlString)

	client := getter.Client{
		Src:     pluginUrlString,
		Dst:     dstFileName,
		Dir:     false,
		Mode:    getter.ClientModeFile,
		Getters: getter.Getters,
		Pwd:     pwd,
	}
	if err := client.Get(); err != nil {
		return "", nil, errors.Trace(err)
	}

	plug, err := plugin.Open(dstFileName)
	if err != nil {
		return "", nil, errors.Annotatef(err, "failed to open go-plugin: %v", dstFileName)
	}

	plugSymbol, err := plug.Lookup(InterfaceName)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	p, ok := plugSymbol.(Plugin)
	if !ok {
		return "", nil, errors.Errorf("go-plugin %s is does not have Plugin interface: %v", dstFileName, reflect.TypeOf(plugSymbol))
	}

	return pluginNameString, p, nil
}
