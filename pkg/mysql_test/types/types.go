package types

import (
	"math/rand"
	"regexp"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type ColumnValGenerator interface {
	ColType() string
	Generate(r *rand.Rand) interface{}
}

var twoR, _ = regexp.Compile(`.+\((\d+),(\d+)\)`)
var onwR, _ = regexp.Compile(`.+\((\d+)\)`)

func twoParamFromType(colType string) (int, int) {
	matches := twoR.FindStringSubmatch(colType)
	if len(matches) != 3 {
		log.Fatalf("[twoParamFromType] invalid col type %s", colType)
	}
	one, err := strconv.Atoi(matches[1])
	if err != nil {
		log.Fatalf("[twoParamFromType] invalid col type %s", colType)
	}
	two, err := strconv.Atoi(matches[2])
	if err != nil {
		log.Fatalf("[twoParamFromType] invalid col type %s", colType)
	}

	return one, two
}

func oneParamFromType(colType string) int {
	matches := onwR.FindStringSubmatch(colType)
	if len(matches) != 2 {
		log.Fatalf("[twoParamFromType] invalid col type %s", colType)
	}
	one, err := strconv.Atoi(matches[1])
	if err != nil {
		log.Fatalf("[twoParamFromType] invalid col type %s", colType)
	}

	return one
}
