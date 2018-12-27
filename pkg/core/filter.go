package core

type IFilter interface {
	Configure(configData map[string]interface{}) error
	Filter(msg *Msg) (continueNext bool, err error)
}

type IFilterFactory interface {
	NewFilter() IFilter
}
