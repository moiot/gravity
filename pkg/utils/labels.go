package utils

import (
	"os"
	"strings"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

func GetLabelEnvString() string {
	return os.Getenv("DRC_LABELS")
}

func GetLabelsFromEnv(envString string) (map[string]string, error) {
	labels := make(map[string]string)

	if envString == "" {
		return labels, nil
	}

	labelPairs := strings.Split(envString, ",")

	for _, pair := range labelPairs {
		pairSlice := strings.Split(pair, "=")
		if len(pairSlice) != 2 {
			return labels, errors.Errorf("invalid label env: %v, pairSlice: %v", envString, pairSlice)
		}

		k := pairSlice[0]
		v := pairSlice[1]
		labels[k] = v
	}
	return labels, nil
}

func MustGetLabelsFromEnv() map[string]string {
	labels, err := GetLabelsFromEnv(GetLabelEnvString())
	if err != nil {
		log.Fatalf("failed to get label: %v", err)
	}
	h, err := os.Hostname()
	if err == nil {
		labels["hostname"] = h
	}
	return labels
}
