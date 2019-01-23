package mongooplog

import "github.com/moiot/gravity/pkg/position_store"

type positionEntity struct {
	Name                         string `bson:"name"`
	Stage                        string `bson:"stage"`
	position_store.MongoPosition `bson:",inline"`
	LastUpdate                   string `bson:"last_update"`
}
