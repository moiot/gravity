package utils

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

// Version information.
var (
	Version   = "None"
	BuildTS   = "None"
	GitHash   = "None"
	GitBranch = "None"
)

// PrintRawInfo prints the version information without log info.
func PrintRawInfo(app string) {
	fmt.Printf("Release Version (%s): %s\n", app, Version)
	fmt.Printf("Git Commit Hash: %s\n", GitHash)
	fmt.Printf("Git Branch: %s\n", GitBranch)
	fmt.Printf("UTC Build Time: %s\n", BuildTS)
}

// LogRotate prints the version information.
func LogRawInfo(app string) {
	log.Infof("Welcome to %s.", app)
	log.Infof("Release Version: %s", Version)
	log.Infof("Git Commit Hash: %s", GitHash)
	log.Infof("Git Branch: %s", GitBranch)
	log.Infof("UTC Build Time: %s", BuildTS)
}
