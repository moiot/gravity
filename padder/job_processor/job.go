package job_processor

import (
	"time"

	gomysql "github.com/siddontang/go-mysql/mysql"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/utils"
)

type Job struct {
	seqNum      int64
	srcId       string
	opType      string
	JobMsg      core.Msg
	pos         gomysql.Position
	gtidSet     gomysql.MysqlGTIDSet
	schemaStore schema_store.SchemaStore
}

func CreateJob(seqNum int64, srcId string, opType string, jobMsg core.Msg, pos gomysql.Position, gtidSet gomysql.MysqlGTIDSet) Job {
	return Job{
		seqNum:  seqNum,
		srcId:   srcId,
		opType:  opType,
		JobMsg:  jobMsg,
		pos:     pos,
		gtidSet: gtidSet,
	}
}

func (job Job) SlidingWindowKey() string {
	return job.srcId
}

func (job Job) TableKey() string {
	return utils.TableIdentity(job.JobMsg.Database, job.JobMsg.Table)
}

func (job Job) WorkerKey() string {
	return job.JobMsg.GetPkSign()
}

func (job Job) EventTime() time.Time {
	return job.JobMsg.Timestamp
}

func (job Job) SequenceNumber() int64 {
	return job.seqNum
}

func (job Job) BeforeWindowMoveForward() {
	return
}

func (job Job) SkipDownStream() bool {
	return false
}

func (job Job) DoneC() chan struct{} {
	return nil
}

func (job Job) Msg() core.Msg {
	return job.JobMsg
}
