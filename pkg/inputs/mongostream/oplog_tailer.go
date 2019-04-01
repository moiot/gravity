package mongostream

import (
	"context"
	"fmt"
	"time"

	"github.com/mitchellh/hashstructure"

	jsoniter "github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/mongo/gtm"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/utils"
)

type OplogTailer struct {
	pipelineName     string
	uniqueSourceName string
	oplogChecker     *OplogChecker
	// mqMsgType         protocol.JobMsgType
	emitter       core.Emitter
	router        core.Router
	ctx           context.Context
	cancel        context.CancelFunc
	idx           int
	session       *mgo.Session
	gtmConfig     *config.GtmConfig
	opCtx         *gtm.OpCtx
	sourceHost    string
	positionCache position_store.PositionCacheInterface
	stopped       bool
}

type isMasterResult struct {
	IsMaster            bool   `json:"ismaster"`
	Msg                 string `json:"msg"`
	MaxBsonObjetSize    int    `json:"maxBsonObjectSize"`
	MaxMessageSizeBytes int    `json:"maxMessageSizeBytes"`
	MaxWriteBatchSize   int    `json:"maxWriteBatchSize"`
	Ok                  string `json:"ok"`
}

type filterOpt struct {
	allowInsert  bool
	allowUpdate  bool
	allowDelete  bool
	allowCommand bool
}

func GetRowDataFromOp(op *gtm.Op) *map[string]interface{} {
	var row *map[string]interface{}
	if op.IsInsert() {
		row = &op.Data
	} else if op.IsUpdate() {
		row = &op.Row
	}

	return row
}

func (tailer *OplogTailer) Filter(op *gtm.Op, option *filterOpt) bool {
	dbName := op.GetDatabase()
	tableName := op.GetCollection()

	// handle heartbeat before route filter
	if op.IsUpdate() && dbName == consts.GravityDBName && tableName == OplogCheckerCollectionName {
		tailer.oplogChecker.MarkActive(op.Data)
	}

	// handle control msg
	if dbName == consts.GravityDBName {
		return true
	}

	m := core.Msg{
		Database: dbName,
		Table:    tableName,
	}
	if !tailer.router.Exists(&m) {
		return false
	}

	return true
}

func (tailer *OplogTailer) Run() {

	log.Infof("running tailer worker idx: %v", tailer.idx)

	tailer.session.SetMode(mgo.Primary, true)

	adminDB := tailer.session.DB("admin")
	result := isMasterResult{}

	adminDB.Run(bson.D{{"isMaster", 1}}, &result)

	log.Infof("[oplog_tailer] isMaster: %v", result)

	positionValue, err := GetPositionValue(tailer.positionCache)
	if err != nil {
		log.Fatalf("[oplogTailer] failed to get position: %v", errors.Trace(err))
	}

	after := func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
		positionValue, err := GetPositionValue(tailer.positionCache)
		if err != nil {
			log.Fatalf("[oplogTailer] failed to get position: %v", errors.Trace(err))
		}
		return bson.MongoTimestamp(positionValue.CurrentPosition)
	}

	// If timestamp is 0, we start from the LastOpTimestamp
	if positionValue.CurrentPosition == config.MongoPosition(0) {
		log.Infof("[oplog_tailer] start from the latest timestamp")
		after = nil
	} else {
		log.Infof("[oplog_tailer] start from the configured timestamp %v", positionValue.CurrentPosition)
	}

	filter := func(op *gtm.Op) bool {
		fopt := filterOpt{allowInsert: true, allowUpdate: true}
		fopt.allowDelete = true
		fopt.allowCommand = true
		return tailer.Filter(op, &fopt)
	}

	options := gtm.Options{After: after, Filter: filter, SourceName: tailer.sourceHost, PipelineName: tailer.pipelineName}

	options.UpdateDataAsDelta = true

	if tailer.gtmConfig != nil {
		log.Infof("[oplog_tailer] gtm-config: %v", tailer.gtmConfig)
		if tailer.gtmConfig.BufferSize > 0 {
			options.BufferSize = tailer.gtmConfig.BufferSize
		}
		if tailer.gtmConfig.ChannelSize > 0 {
			options.ChannelSize = tailer.gtmConfig.ChannelSize
		}

		options.BufferDuration = time.Millisecond * time.Duration(tailer.gtmConfig.BufferDurationMs)
	}

	tailer.opCtx = gtm.Start(tailer.session, &options)
	for {
		select {
		case err := <-tailer.opCtx.ErrC:
			log.Fatalf("[oplog_tailer] err: %v", err)
		case op := <-tailer.opCtx.OpC:
			received := time.Now()
			if mongo.IsDeadSignal(op, tailer.pipelineName) {
				log.Info("[oplog_tailer] receive dead signal, exit")
				tailer.Stop()
			}

			msg := core.Msg{
				Phase: core.Phase{
					EnterInput: received,
				},
				Host:      tailer.sourceHost,
				Database:  op.GetDatabase(),
				Table:     op.GetCollection(),
				Timestamp: time.Unix(int64(op.Timestamp)>>32, 0),
				Oplog:     op,
				Done:      make(chan struct{}),
			}
			var c prometheus.Counter

			if op.IsCommand() {
				stmt, err := jsoniter.MarshalToString(op.Data)
				if err != nil {
					log.Fatalf("[oplog_tailer] fail to marshal command. data: %v, err: %s", op.Data, err)
				}
				msg.Type = core.MsgDDL
				msg.DdlMsg = &core.DDLMsg{
					Statement: stmt,
				}
				c = metrics.InputCounter.WithLabelValues(tailer.pipelineName, msg.Database, msg.Table, string(msg.Type), "")
			} else {
				var o core.DMLOp
				var data map[string]interface{}
				if op.IsInsert() {
					o = core.Insert
					data = op.Data
				} else if op.IsUpdate() {
					o = core.Update
					data = op.Row
				} else if op.IsDelete() {
					o = core.Delete
				}
				msg.Type = core.MsgDML
				msg.DmlMsg = &core.DMLMsg{
					Operation: o,
					Data:      data,
					Old:       make(map[string]interface{}),
					Pks: map[string]interface{}{
						"_id": op.Id,
					},
				}
				c = metrics.InputCounter.WithLabelValues(tailer.pipelineName, msg.Database, msg.Table, string(msg.Type), string(o))
			}
			c.Add(1)
			msg.InputStreamKey = utils.NewStringPtr("mongooplog")
			idKey := outputStreamKey(msg.Oplog)
			h, err := hashstructure.Hash(idKey, nil)
			if err != nil {
				log.Fatalf("failed to generate hash: %v", err.Error())
			}
			msg.OutputDepHashes = []core.OutputHash{{idKey, h}}
			msg.InputContext = config.MongoPosition(op.Timestamp)
			msg.AfterCommitCallback = tailer.AfterMsgCommit
			if err := tailer.emitter.Emit(&msg); err != nil {
				log.Fatalf("failed to emit: %v", errors.ErrorStack(err))
			}

		case <-tailer.ctx.Done():
			tailer.Stop()
			return
		}
	}
}

func (tailer *OplogTailer) AfterMsgCommit(msg *core.Msg) error {
	position, ok := msg.InputContext.(config.MongoPosition)
	if !ok {
		return errors.Errorf("invalid InputContext")
	}

	if err := UpdateCurrentPositionValue(tailer.positionCache, position); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func outputStreamKey(oplog *gtm.Op) string {
	switch id := oplog.Id.(type) {

	case bson.ObjectId:
		return id.String()

	case string:
		return id

	default:
		panic(fmt.Sprintf("unknown id type %#v", id))
	}
}

func (tailer *OplogTailer) SendDeadSignal() error {
	return mongo.SendDeadSignal(tailer.session, tailer.pipelineName)
}

func (tailer *OplogTailer) Wait() {
	<-tailer.ctx.Done()
}

func (tailer *OplogTailer) Stop() {
	if tailer.stopped {
		return
	}
	log.Infof("[oplog_tailer]: stop idx: %v", tailer.idx)
	tailer.stopped = true
	tailer.cancel()
}

type OplogTailerOpt struct {
	pipelineName     string
	uniqueSourceName string
	// mqMsgType        protocol.JobMsgType
	gtmConfig     *config.GtmConfig
	session       *mgo.Session
	oplogChecker  *OplogChecker
	sourceHost    string
	positionCache position_store.PositionCacheInterface
	emitter       core.Emitter
	router        core.Router
	logger        log.Logger
	idx           int
	ctx           context.Context
}

func NewOplogTailer(opts *OplogTailerOpt) *OplogTailer {
	if opts.pipelineName == "" {
		log.Fatalf("[oplog_tailer] pipeline name is empty")
	}

	tailer := OplogTailer{
		pipelineName:     opts.pipelineName,
		uniqueSourceName: opts.uniqueSourceName,
		session:          opts.session,
		oplogChecker:     opts.oplogChecker,
		gtmConfig:        opts.gtmConfig,
		emitter:          opts.emitter,
		router:           opts.router,
		idx:              opts.idx,
		sourceHost:       opts.sourceHost,
		positionCache:    opts.positionCache,
	}
	tailer.ctx, tailer.cancel = context.WithCancel(opts.ctx)
	log.Infof("[oplog_tailer] tailer created")
	return &tailer
}
