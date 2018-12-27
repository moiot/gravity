package main

type filterPlugin struct {
	pipelineName string
	pluginName   string
	url          string
}

// exported variable
var Plugin = filterPlugin{}

func (p *filterPlugin) Configure(pipelineName string, data map[string]interface{}) error {
	p.pipelineName = pipelineName

	p.pluginName = data["name"].(string)
	p.url = data["url"].(string)
	return nil
}
