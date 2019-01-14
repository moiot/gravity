package position_store

import (
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
)

type MongoPosition struct {
	StartPosition   config.MongoPosition `json:"start_position" bson:"start_position"`
	CurrentPosition config.MongoPosition `json:"current_position" bson:"current_position"`
}

func (p MongoPosition) toJson() string {
	s, err := myJson.MarshalToString(p)
	if err != nil {
		log.Fatal(err)
	}
	return s
}
