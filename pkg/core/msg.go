package core

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/juju/errors"

	"github.com/pingcap/parser/ast"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/mongo/gtm"
)

var PipelineName string

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
	Phase

	Type     MsgType
	Host     string
	Database string
	Table    string

	DdlMsg *DDLMsg
	DmlMsg *DMLMsg

	//
	// Timestamp, TimeZone, Oplog will be deprecated.
	//
	Timestamp time.Time // event generated at source
	TimeZone  *time.Location
	Oplog     *gtm.Op

	InputStreamKey  *string
	OutputStreamKey *string
	Done            chan struct{}

	InputSequence *int64

	InputContext        interface{}
	AfterCommitCallback AfterMsgCommitFunc
}

func (msg *Msg) SequenceNumber() int64 {
	return *msg.InputSequence
}

func (msg *Msg) BeforeWindowMoveForward() {
	if msg.AfterCommitCallback != nil {
		if err := msg.AfterCommitCallback(msg); err != nil {
			log.Fatalf("callback failed: %v", errors.ErrorStack(err))
		}
	}
	close(msg.Done)
}

func (msg *Msg) EventTime() time.Time {
	return msg.Timestamp
}

func (msg *Msg) ProcessTime() time.Time {
	return msg.EnterInput
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

type Phase struct {
	EnterInput     time.Time
	EnterEmitter   time.Time // also leave input
	LeaveEmitter   time.Time
	EnterScheduler time.Time // also enter submitter
	LeaveScheduler time.Time // also leave acker
	LeaveSubmitter time.Time
	EnterAcker     time.Time // also leave output
	EnterOutput    time.Time
	//Committed      time.Time not used
}
