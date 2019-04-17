package mongokafka_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/app"
	gravityConfig "github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core/encoding"
	"github.com/moiot/gravity/pkg/mongo_test"
)

type person struct {
	Name string
}

func TestStream(t *testing.T) {
	r := require.New(t)

	mongoCfg := mongo_test.TestConfig()
	pipelineConfig := gravityConfig.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      gravityConfig.PipelineConfigV3Version,
		InputPlugin: gravityConfig.InputConfig{
			Type: "mongo",
			Mode: "stream",
			Config: map[string]interface{}{
				"source": map[string]interface{}{
					"host":     mongoCfg.Host,
					"port":     mongoCfg.Port,
					"username": mongoCfg.Username,
					"password": mongoCfg.Password,
					"database": mongoCfg.Database,
				},
			},
		},
		OutputPlugin: gravityConfig.GenericPluginConfig{
			Type: "async-kafka",
			Config: map[string]interface{}{
				"kafka-global-config": map[string]interface{}{
					"broker-addrs": kafkaBroker,
					"mode":         "async"},
				"routes": []map[string]interface{}{
					{
						"match-schema": db.Name,
						"match-table":  t.Name(),
						"dml-topic":    t.Name(),
					},
				},
			},
		},
	}

	messageC := newKafkaConsumer(t.Name())
	const operations = 30

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)
	r.NoError(server.Start())

	coll := db.C(t.Name())

	for i := 0; i < operations; i++ {
		r.NoError(coll.Insert(&person{"Foo" + strconv.Itoa(i)}))
	}

	// should ignore
	coll = db.C(t.Name() + "_2")
	r.NoError(coll.Insert(&person{"Foo"}))

	r.NoError(server.Input.SendDeadSignal())
	server.Input.Wait()
	server.Close()

	for i := 0; i < operations; i++ {
		select {
		case val := <-messageC:
			j := encoding.JsonMsg01{}
			r.NoError(json.Unmarshal([]byte(val), &j))
			r.Equal(encoding.Version01, j.Version)
			r.Equal(db.Name, j.Database)
			r.Equal(t.Name(), j.Collection)
			r.Equal("Foo"+strconv.Itoa(i), j.Oplog.Data["name"])

		case <-time.After(5 * time.Second):
			log.Panic("time out no message received")
		}
	}

	select {
	case m := <-messageC:
		log.Panicf("receive unexpected msg %s", m)

	case <-time.After(time.Second):
	}
}
