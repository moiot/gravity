package stages

type InputStage string

const (
	InputStageFull        InputStage = "full"
	InputStageIncremental InputStage = "incremental"
)
