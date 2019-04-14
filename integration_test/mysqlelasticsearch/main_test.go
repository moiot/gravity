package mysqlelasticsearch_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/elasticsearch_test"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/olivere/elastic"
)

func TestMain(m *testing.M) {
	db := mysql_test.MustCreateSourceDBConn()
	_, err := db.Exec("drop database if exists " + consts.GravityDBName)
	if err != nil {
		panic(err)
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}

	client, err := elasticsearch_test.CreateTestClient()
	if err != nil {
		panic(err)
	}
	_, err = client.DeleteIndex(mysql_test.TestTableName).Do(context.Background())
	if err != nil {
		e, ok := err.(*elastic.Error)
		if !ok || e.Status != http.StatusNotFound {
			panic(err)
		}
	}
	client.Stop()
	os.Exit(m.Run())
}
