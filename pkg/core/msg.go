package core

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/mongo/gtm"
)

type MsgType string

const (
	MsgDML MsgType = "dml"
	MsgDDL MsgType = "ddl"

	// ctl message is internal messages like
	// heartbeat, barrier that is not dml/ddl.
	MsgCtl MsgType = "ctl"

	// MsgCloseInputStream is used to tell the scheduler to close a stream
	MsgCloseInputStream MsgType = "closeInput"
)

type DMLOp string

const (
	Insert DMLOp = "insert"
	Update DMLOp = "update"
	Delete DMLOp = "delete"
)

type AfterMsgCommitFunc func(m *Msg) error

type Msg struct {
	Metrics

	Type     MsgType
	Host     string
	Database string
	Table    string

	DdlMsg *DDLMsg
	DmlMsg *DMLMsg

	//
	// Timestamp, TimeZone, Oplog will be deprecated.
	//
	Timestamp time.Time
	TimeZone  *time.Location
	Oplog     *gtm.Op

	InputStreamKey  *string
	OutputStreamKey *string
	Done            chan struct{}

	InputSequence *int64

	InputContext        interface{}
	AfterCommitCallback AfterMsgCommitFunc
}

type MsgSubmitter interface {
	SubmitMsg(msg *Msg) error
}

type MsgAcker interface {
	AckMsg(msg *Msg) error
}

func (msg Msg) GetPkSign() string {
	var sign string
	for _, v := range msg.DmlMsg.Pks {
		pkString := fmt.Sprint(v)
		if pkString == "" {
			log.Warnf("[mysql/job_msg] GetPartitionKeyFromPayload: empty primary key")
		}
		sign += pkString + "_"
	}
	return sign
}

type DDLMsg struct {
	Statement string
	AST       ast.StmtNode
}

type DMLMsg struct {
	Operation DMLOp
	Data      map[string]interface{}
	Old       map[string]interface{}
	Pks       map[string]interface{}
}

type TaskReportStage string

const (
	ReportStageFull        TaskReportStage = "Full"
	ReportStageIncremental TaskReportStage = "Incremental"
)

type TaskReportStatus struct {
	Name       string          `json:"name"`
	ConfigHash string          `json:"configHash"`
	Position   string          `json:"position"`
	Stage      TaskReportStage `json:"stage"`
	Version    string          `json:"version"`
}

func HashConfig(config string) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(config))
	return SafeEncodeString(fmt.Sprint(hasher.Sum32()))
}

const alphanums = "bcdfghjklmnpqrstvwxz2456789"

func SafeEncodeString(s string) string {
	r := make([]byte, len(s))
	for i, b := range []rune(s) {
		r[i] = alphanums[(int(b) % len(alphanums))]
	}
	return string(r)
}

//
// metrics definitions
//
type Metrics struct {
	MsgCreateTime time.Time
	MsgEmitTime   time.Time
	MsgSubmitTime time.Time
	MsgAckTime    time.Time
}

const (
	PipelineTag = "pipeline"
)

var (
	MsgCreateToEmitDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "msg_create_to_emit_duration",
			Help:      "Bucketed histogram of processing time (s) from msg create to msg emit",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{PipelineTag})

	MsgEmitToSubmitDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "msg_emit_to_submit_duration",
			Help:      "Bucketed histogram of processing time (s) from msg emit to submit",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{PipelineTag})

	MsgSubmitToAckDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "msg_submit_to_ack_duration",
			Help:      "Bucketed histogram of processing time (s) from msg submit to ack",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{PipelineTag})

	MsgCreateToAckDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "msg_create_to_ack_duration",
			Help:      "Bucketed histogram of processing time (s) from msg create to ack",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{PipelineTag})
)

func AddMetrics(pipelineName string, msg *Msg) {
	msgCreateToEmitDuration := msg.MsgEmitTime.Sub(msg.MsgCreateTime).Seconds()
	msgEmitToSubmitDuration := msg.MsgSubmitTime.Sub(msg.MsgEmitTime).Seconds()
	msgSubmitToAckDuration := msg.MsgAckTime.Sub(msg.MsgSubmitTime).Seconds()
	msgCreateToAckDuration := msg.MsgAckTime.Sub(msg.MsgCreateTime).Seconds()

	MsgCreateToEmitDurationHistogram.WithLabelValues(pipelineName).Observe(msgCreateToEmitDuration)
	MsgEmitToSubmitDurationHistogram.WithLabelValues(pipelineName).Observe(msgEmitToSubmitDuration)
	MsgSubmitToAckDurationHistogram.WithLabelValues(pipelineName).Observe(msgSubmitToAckDuration)
	MsgCreateToAckDurationHistogram.WithLabelValues(pipelineName).Observe(msgCreateToAckDuration)
}

func init() {
	prometheus.MustRegister(MsgCreateToEmitDurationHistogram)
	prometheus.MustRegister(MsgEmitToSubmitDurationHistogram)
	prometheus.MustRegister(MsgSubmitToAckDurationHistogram)
	prometheus.MustRegister(MsgCreateToAckDurationHistogram)
}
