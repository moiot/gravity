package elasticsearch_test

import (
	"os"
	"strings"

	"github.com/olivere/elastic"
)

func TestURLs() []string {
	urls, ok := os.LookupEnv("ELASTICSEARCH_URLS")
	if !ok {
		return []string{"http://127.0.0.1:9200"}
	}
	return strings.Split(urls, ",")
}

func CreateTestClient() (*elastic.Client, error) {
	return elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(TestURLs()...))
}
