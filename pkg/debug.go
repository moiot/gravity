package pkg

import "flag"

type GoRuntimeDebug struct {
	BlockProfileRate int
}

func InitDebugFlagSet(fs *flag.FlagSet, debug *GoRuntimeDebug) {
	fs.IntVar(&debug.BlockProfileRate, "blockprofilerate", 0, "Turn on block profiling with the given rate")
}
