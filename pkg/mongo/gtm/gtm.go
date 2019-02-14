package gtm

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/serialx/hashring"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type OrderingGuarantee int

const (
	Oplog     OrderingGuarantee = iota // ops sent in oplog order (strong ordering)
	Namespace                          // ops sent in oplog order within a namespace
	Document                           // ops sent in oplog order for a single document
)

type QuerySource int

const (
	OplogQuerySource QuerySource = iota
	DirectQuerySource
)

type Options struct {
	PipelineName        string
	SourceName          string
	After               TimestampGenerator
	Filter              OpFilter
	OpLogDatabaseName   *string
	OpLogCollectionName *string
	CursorTimeout       *string
	ChannelSize         int
	UseBufferDuration   bool
	BufferSize          int
	BufferDuration      time.Duration
	Ordering            OrderingGuarantee
	WorkerCount         int
	UpdateDataAsDelta   bool
	DirectReadNs        []string
	DirectReadersPerCol int
	DirectReadLimit     int
	DirectReadFilter    OpFilter
	DirectReadBatchSize int
}

type Op struct {
	Id        interface{} `json:"_id"`
	Operation string      `json:"operation"`
	Namespace string      `json:"namespace"`
	// Data is the change filed in oplog
	Data map[string]interface{} `json:"data"`
	// Row is the data of this row
	Row       map[string]interface{} `json:"row"`
	Timestamp bson.MongoTimestamp    `json:"-"`
	Source    QuerySource            `json:"source"`
}

type OpLog struct {
	Timestamp    bson.MongoTimestamp "ts"
	HistoryID    int64               "h"
	MongoVersion int                 "v"
	Operation    string              "op"
	Namespace    string              "ns"
	Doc          *bson.Raw           "o"
	Update       *bson.Raw           "o2"
}

type OpChan chan *Op

type OpLogEntry map[string]interface{}

type OpFilter func(*Op) bool

type TimestampGenerator func(*mgo.Session, *Options) bson.MongoTimestamp

type OpBuf struct {
	UseBufferDuration bool
	Entries           []*Op
	BufferSize        int
	BufferDuration    time.Duration
	FlushTicker       *time.Ticker
}

type OpCtx struct {
	pipelineName string
	OpC          OpChan
	ErrC         chan error
	DirectReadWg *sync.WaitGroup
	routines     int
	stopC        chan bool
	allWg        *sync.WaitGroup
	seekC        chan bson.MongoTimestamp
	pauseC       chan bool
	resumeC      chan bool
	paused       bool
}

func (ctx *OpCtx) Since(ts bson.MongoTimestamp) {
	ctx.seekC <- ts
}

func (ctx *OpCtx) Pause() {
	if !ctx.paused {
		ctx.paused = true
		ctx.pauseC <- true
	}
}

func (ctx *OpCtx) Resume() {
	if ctx.paused {
		ctx.paused = false
		ctx.resumeC <- true
	}
}

func (ctx *OpCtx) Stop() {
	for i := 1; i <= ctx.routines; i++ {
		ctx.stopC <- true
	}
	ctx.allWg.Wait()
}

func ChainOpFilters(filters ...OpFilter) OpFilter {
	return func(op *Op) bool {
		for _, filter := range filters {
			if filter(op) == false {
				return false
			}
		}
		return true
	}
}

func (this *Op) MarshalJSON() ([]byte, error) {
	type Alias Op
	return json.Marshal(&struct {
		Timestamp int32 `json:"timestamp"`
		Ordinal   int32 `json:"ordinal"`
		*Alias
	}{
		Timestamp: int32(int64(this.Timestamp) >> 32),
		Ordinal:   int32(this.Timestamp),
		Alias:     (*Alias)(this),
	})
}

func (this *Op) IsDrop() bool {
	if _, drop := this.IsDropDatabase(); drop {
		return true
	}
	if _, drop := this.IsDropCollection(); drop {
		return true
	}
	return false
}

func (this *Op) IsDropCollection() (string, bool) {
	if this.IsCommand() {
		if this.Data != nil {
			if val, ok := this.Data["drop"]; ok {
				return val.(string), true
			}
		}
	}
	return "", false
}

func (this *Op) IsDropDatabase() (string, bool) {
	if this.IsCommand() {
		if this.Data != nil {
			if _, ok := this.Data["dropDatabase"]; ok {
				return this.GetDatabase(), true
			}
		}
	}
	return "", false
}

func (this *Op) IsCommand() bool {
	return this.Operation == "c"
}

func (this *Op) IsInsert() bool {
	return this.Operation == "i"
}

func (this *Op) IsUpdate() bool {
	return this.Operation == "u"
}

func (this *Op) IsDelete() bool {
	return this.Operation == "d"
}

func (this *Op) IsSourceOplog() bool {
	return this.Source == OplogQuerySource
}

func (this *Op) IsSourceDirect() bool {
	return this.Source == DirectQuerySource
}

func (this *Op) ParseNamespace() []string {
	return strings.SplitN(this.Namespace, ".", 2)
}

func (this *Op) GetDatabase() string {
	return this.ParseNamespace()[0]
}

func (this *Op) GetCollection() string {
	if _, drop := this.IsDropDatabase(); drop {
		return ""
	} else if col, drop := this.IsDropCollection(); drop {
		return col
	} else {
		return this.ParseNamespace()[1]
	}
}

func (this *OpBuf) Append(op *Op) {
	this.Entries = append(this.Entries, op)
}

func (this *OpBuf) IsFull() bool {
	return len(this.Entries) >= this.BufferSize
}

func (this *OpBuf) Flush(session *mgo.Session, ctx *OpCtx) {
	if len(this.Entries) == 0 {
		return
	}
	ns := make(map[string][]interface{})
	byId := make(map[interface{}][]*Op)
	for _, op := range this.Entries {
		if op.IsUpdate() {
			idKey := fmt.Sprintf("%s.%v", op.Namespace, op.Id)
			ns[op.Namespace] = append(ns[op.Namespace], op.Id)
			byId[idKey] = append(byId[idKey], op)
		}
	}
	for n, opIds := range ns {
		var parts = strings.SplitN(n, ".", 2)
		var results []map[string]interface{}
		db, col := parts[0], parts[1]
		sel := bson.M{"_id": bson.M{"$in": opIds}}
		collection := session.DB(db).C(col)
		err := collection.Find(sel).All(&results)
		if err == nil {
			for _, result := range results {
				resultId := fmt.Sprintf("%s.%v", n, result["_id"])
				if ops, ok := byId[resultId]; ok {
					if len(ops) == 1 {
						//ops[0].Data = result
						ops[0].Row = result
					} else {
						for _, o := range ops {
							data := make(map[string]interface{})
							for k, v := range result {
								data[k] = v
							}
							//o.Data = data
							o.Row = data
						}
					}
				}
			}
		} else {
			ctx.ErrC <- err
		}
	}
	for _, op := range this.Entries {
		ctx.OpC <- op
	}
	this.Entries = nil
}

func UpdateIsReplace(entry map[string]interface{}) bool {
	if _, ok := entry["$set"]; ok {
		return false
	} else if _, ok := entry["$unset"]; ok {
		return false
	} else {
		return true

	}
}

func (this *Op) ParseLogEntry(entry *OpLog, options *Options) (include bool) {
	var rawField *bson.Raw
	var objectField map[string]interface{}
	this.Operation = entry.Operation
	this.Timestamp = entry.Timestamp
	this.Namespace = entry.Namespace

	if this.IsInsert() || this.IsDelete() || this.IsUpdate() {
		if this.IsUpdate() {
			rawField = entry.Update
			rawField.Unmarshal(&objectField)
		} else {
			rawField = entry.Doc
			rawField.Unmarshal(&objectField)
		}

		this.Id = objectField["_id"]
		if this.IsInsert() {
			this.Data = objectField
		} else if this.IsUpdate() {
			var changeField map[string]interface{}
			rawField = entry.Doc
			rawField.Unmarshal(&changeField)
			this.Data = changeField
		}
		include = true
	} else if this.IsCommand() {
		rawField = entry.Doc
		rawField.Unmarshal(&objectField)
		this.Data = objectField
		include = this.IsDrop()
	} else {
		include = false
	}
	return
}

func OpLogCollectionName(session *mgo.Session, options *Options) string {
	localDB := session.DB(*options.OpLogDatabaseName)
	col_names, err := localDB.CollectionNames()
	if err == nil {
		var col_name *string = nil
		for _, name := range col_names {
			if strings.HasPrefix(name, "oplog.") {
				col_name = &name
				break
			}
		}
		if col_name == nil {
			msg := fmt.Sprintf(`
				Unable to find oplog collection 
				in database %v`, *options.OpLogDatabaseName)
			panic(msg)
		} else {
			return *col_name
		}
	} else {
		msg := fmt.Sprintf(`Unable to get collection names 
		for database %v: %s, sourceHost: %s`, *options.OpLogDatabaseName, err, options.SourceName)
		panic(msg)
	}
}

func OpLogCollection(session *mgo.Session, options *Options) *mgo.Collection {
	localDB := session.DB(*options.OpLogDatabaseName)
	return localDB.C(*options.OpLogCollectionName)
}

func ParseTimestamp(timestamp bson.MongoTimestamp) (int32, int32) {
	ordinal := (timestamp << 32) >> 32
	ts := (timestamp >> 32)
	return int32(ts), int32(ordinal)
}

func LastOpTimestamp(session *mgo.Session, options *Options) bson.MongoTimestamp {
	var opLog OpLog
	collection := OpLogCollection(session, options)
	collection.Find(nil).Sort("-$natural").One(&opLog)
	return opLog.Timestamp
}

func GetOpLogQuery(session *mgo.Session, after bson.MongoTimestamp, options *Options) *mgo.Query {
	query := bson.M{"ts": bson.M{"$gt": after}, "fromMigrate": bson.M{"$exists": false}}
	collection := OpLogCollection(session, options)
	return collection.Find(query).LogReplay().Sort("$natural")
}

func TailOps(ctx *OpCtx, session *mgo.Session, channels []OpChan, options *Options) error {
	log.Infof("[gtm] TailOps start")
	defer ctx.allWg.Done()
	s := session.Copy()
	defer s.Close()
	options.Fill(s, options.SourceName)
	duration, err := time.ParseDuration(*options.CursorTimeout)
	if err != nil {
		panic(fmt.Sprintf("Invalid value <%s> for CursorTimeout", *options.CursorTimeout))
	}
	currTimestamp := options.After(s, options)
	iter := GetOpLogQuery(s, currTimestamp, options).Tail(duration)
	for {
		var entry OpLog
	Seek:
		for {
			r := iter.Next(&entry)
			if r == false {
				if err := iter.Err(); err != nil {
					ctx.ErrC <- err
				}
			}
			op := &Op{
				Id:        "",
				Operation: "",
				Namespace: "",
				Data:      nil,
				Timestamp: bson.MongoTimestamp(0),
				Source:    OplogQuerySource,
			}

			if op.ParseLogEntry(&entry, options) {
				if options.Filter == nil || options.Filter(op) {
					// broadcast to fetch channels
					for _, channel := range channels {
						channel <- op
					}
					//if options.UpdateDataAsDelta {
					//	ctx.OpC <- op
					//} else {
					//	// broadcast to fetch channels
					//	for _, channel := range channels {
					//		channel <- op
					//	}
					//}
				}
			}
			select {
			case <-ctx.stopC:
				return nil
			case ts := <-ctx.seekC:
				currTimestamp = ts
				break Seek
			case <-ctx.pauseC:
				<-ctx.resumeC
				select {
				case <-ctx.stopC:
					return nil
				case ts := <-ctx.seekC:
					currTimestamp = ts
					break Seek
				default:
					currTimestamp = op.Timestamp
				}
			default:
				currTimestamp = op.Timestamp
			}
		}
		if err = iter.Close(); err != nil {
			ctx.ErrC <- err
		}
		if iter.Timeout() {
			select {
			case <-ctx.stopC:
				return nil
			case ts := <-ctx.seekC:
				currTimestamp = ts
			case <-ctx.pauseC:
				<-ctx.resumeC
				select {
				case ts := <-ctx.seekC:
					currTimestamp = ts
				default:
					continue
				}
			default:
				continue
			}
		}
		iter = GetOpLogQuery(s, currTimestamp, options).Tail(duration)
	}
	return nil
}

func DirectRead(ctx *OpCtx, session *mgo.Session, idx int, ns string, options *Options) (err error) {
	defer ctx.allWg.Done()
	defer ctx.DirectReadWg.Done()
	s := session.Copy()
	defer s.Close()

	log.Infof("[gtm] DirectRead start")
	skip, limit := idx*options.DirectReadLimit, options.DirectReadLimit
	dbCol := strings.SplitN(ns, ".", 2)
	if len(dbCol) != 2 {
		err = fmt.Errorf("Invalid direct read ns: %s :expecting db.collection", ns)
		ctx.ErrC <- err
		return
	}
	db, col := dbCol[0], dbCol[1]
	c := s.DB(db).C(col)
	q := c.Find(nil).Limit(limit).Sort("_id").Hint("_id").Batch(options.DirectReadBatchSize)
	for {
		q.Skip(skip)
		iter := q.Iter()
		if iter.Done() {
			if err = iter.Close(); err != nil {
				ctx.ErrC <- err
			}
			break
		}
		result := make(map[string]interface{})
		for iter.Next(&result) {
			op := &Op{
				Id:        result["_id"],
				Operation: "i",
				Namespace: ns,
				Data:      result,
				Source:    DirectQuerySource,
			}
			switch op.Id.(type) {
			case bson.ObjectId:
				// set timestamp based on id
				t := op.Id.(bson.ObjectId).Time().UTC().Unix()
				op.Timestamp = bson.MongoTimestamp(t << 32)
			}
			if options.DirectReadFilter == nil || options.DirectReadFilter(op) {
				ctx.OpC <- op
			}
			result = make(map[string]interface{})
		}
		if err = iter.Close(); err != nil {
			ctx.ErrC <- err
			break
		}
		skip = skip + (limit * options.DirectReadersPerCol)
		select {
		case <-ctx.stopC:
			return
		default:
			continue
		}

	}
	return
}

func FetchDocuments(ctx *OpCtx, session *mgo.Session, filter OpFilter, buf *OpBuf, inOp OpChan) error {

	log.Infof("[gtm] FetchDocuments start")
	defer ctx.allWg.Done()
	s := session.Copy()
	defer s.Close()
	if buf.UseBufferDuration {
		log.Infof("[gtm] UseBufferDuration")
		for {
			select {
			case <-ctx.stopC:
				return nil
			case <-buf.FlushTicker.C:
				buf.Flush(s, ctx)
			case op := <-inOp:
				if filter(op) {
					buf.Append(op)
					if buf.IsFull() {
						buf.Flush(s, ctx)
						buf.FlushTicker.Stop()
						buf.FlushTicker = time.NewTicker(buf.BufferDuration)
					}
				}
			}
		}
	} else {
		log.Infof("[gtm] DoNotUseBufferDuration")
		for {
			select {
			case <-ctx.stopC:
				return nil
			case op := <-inOp:
				if filter(op) {
					buf.Append(op)
					if buf.IsFull() || len(inOp) == 0 {
						buf.Flush(s, ctx)
					}
				}
			}
		}
	}
	return nil
}

func OpFilterForOrdering(ordering OrderingGuarantee, workers []string, worker string) OpFilter {
	switch ordering {
	case Document:
		ring := hashring.New(workers)
		return func(op *Op) bool {
			var key string
			if op.Id != nil {
				key = fmt.Sprintf("%v", op.Id)
			} else {
				key = op.Namespace
			}
			if who, ok := ring.GetNode(key); ok {
				return who == worker
			} else {
				return false
			}
		}
	case Namespace:
		ring := hashring.New(workers)
		return func(op *Op) bool {
			if who, ok := ring.GetNode(op.Namespace); ok {
				return who == worker
			} else {
				return false
			}
		}
	default:
		return func(op *Op) bool {
			return true
		}
	}
}

func DefaultOptions() *Options {
	return &Options{
		After:               nil,
		Filter:              nil,
		OpLogDatabaseName:   nil,
		OpLogCollectionName: nil,
		CursorTimeout:       nil,
		ChannelSize:         512,
		BufferSize:          50,
		BufferDuration:      time.Duration(750) * time.Millisecond,
		Ordering:            Oplog,
		WorkerCount:         1,
		UpdateDataAsDelta:   true,
		DirectReadNs:        []string{},
		DirectReadLimit:     5000,
		DirectReadersPerCol: 10,
		DirectReadFilter:    nil,
		DirectReadBatchSize: 500,
	}
}

func (this *Options) Fill(session *mgo.Session, sourceHost string) {
	if this.After == nil {
		this.After = LastOpTimestamp
	}
	if this.OpLogDatabaseName == nil {
		defaultOpLogDatabaseName := "local"
		this.OpLogDatabaseName = &defaultOpLogDatabaseName
	}
	if this.OpLogCollectionName == nil {
		defaultOpLogCollectionName := OpLogCollectionName(session, this)
		this.OpLogCollectionName = &defaultOpLogCollectionName
	}
	if this.CursorTimeout == nil {
		defaultCursorTimeout := "100s"
		this.CursorTimeout = &defaultCursorTimeout
	}
}

func (this *Options) SetDefaults() {
	defaultOpts := DefaultOptions()
	if this.ChannelSize < 1 {
		this.ChannelSize = defaultOpts.ChannelSize
	}
	if this.BufferSize < 1 {
		this.BufferSize = defaultOpts.BufferSize
	}
	if this.BufferDuration == 0 {
		this.BufferDuration = defaultOpts.BufferDuration
	}
	if this.Ordering == Oplog {
		this.WorkerCount = 1
	}
	if this.WorkerCount < 1 {
		this.WorkerCount = 1
	}

	if this.DirectReadLimit == 0 {
		this.DirectReadLimit = defaultOpts.DirectReadLimit
	}
	if this.DirectReadersPerCol == 0 {
		this.DirectReadersPerCol = defaultOpts.DirectReadersPerCol
	}
	if this.DirectReadBatchSize < 1 {
		this.DirectReadBatchSize = defaultOpts.DirectReadBatchSize
	}
}

func Tail(session *mgo.Session, options *Options) (OpChan, chan error) {
	ctx := Start(session, options)
	return ctx.OpC, ctx.ErrC
}

func Start(session *mgo.Session, options *Options) *OpCtx {
	if options == nil {
		options = DefaultOptions()
	} else {
		options.SetDefaults()
	}

	routines := options.WorkerCount + (len(options.DirectReadNs) * options.DirectReadersPerCol) + 1
	stopC := make(chan bool, routines)
	errC := make(chan error, options.ChannelSize)
	opC := make(OpChan, options.ChannelSize)

	var inOps []OpChan
	var workerNames []string
	var directReadWg sync.WaitGroup
	var allWg sync.WaitGroup
	var seekC = make(chan bson.MongoTimestamp, 1)
	var pauseC = make(chan bool, 1)
	var resumeC = make(chan bool, 1)

	ctx := &OpCtx{
		pipelineName: options.PipelineName,
		OpC:          opC,
		ErrC:         errC,
		DirectReadWg: &directReadWg,
		routines:     routines,
		stopC:        stopC,
		allWg:        &allWg,
		pauseC:       pauseC,
		resumeC:      resumeC,
		seekC:        seekC,
	}

	for i := 1; i <= options.WorkerCount; i++ {
		workerNames = append(workerNames, strconv.Itoa(i))
	}

	for i := 1; i <= options.WorkerCount; i++ {
		allWg.Add(1)
		inOp := make(OpChan, options.ChannelSize)
		inOps = append(inOps, inOp)
		buf := &OpBuf{
			UseBufferDuration: options.UseBufferDuration,
			BufferSize:        options.BufferSize,
			BufferDuration:    options.BufferDuration,
			FlushTicker:       time.NewTicker(options.BufferDuration),
		}
		worker := strconv.Itoa(i)
		filter := OpFilterForOrdering(options.Ordering, workerNames, worker)
		go FetchDocuments(ctx, session, filter, buf, inOp)
	}

	for _, ns := range options.DirectReadNs {
		for i := 0; i < options.DirectReadersPerCol; i++ {
			directReadWg.Add(1)
			allWg.Add(1)
			go DirectRead(ctx, session, i, ns, options)
		}
	}

	allWg.Add(1)
	go TailOps(ctx, session, inOps, options)

	return ctx
}
