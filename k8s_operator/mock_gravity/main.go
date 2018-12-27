package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
)

var myJson = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

var version int

func init() {
	flag.IntVar(&version, "v", 0, "version")
}

func main() {
	flag.Parse()

	http.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		ret := core.TaskReportStatus{}
		resp, err := myJson.Marshal(ret)
		if err != nil {
			log.Error(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
