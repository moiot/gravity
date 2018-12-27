gtm
===
gtm (go tail mongo) is a utility written in Go which tails the MongoDB oplog and sends create, update, delete events to your code.
It can be used to send emails to new users, [index documents](https://www.github.com/rwynn/monstache), 
[write time series data](https://www.github.com/rwynn/mongofluxd), or something else.

### Requirements ###
+ [Go](http://golang.org/doc/install)
+ [mgo](http://labix.org/mgo), the mongodb driver for Go
+ [mongodb](http://www.mongodb.org/)
	+ Pass argument --master to mongod to ensure an oplog is created OR
	+ Setup [replica sets](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/) to create oplog

### Installation ###

	go get github.com/rwynn/gtm

### Usage ###
	
	package main
	
	import "gopkg.in/mgo.v2"
	import "gopkg.in/mgo.v2/bson"
	import "github.com/rwynn/gtm"
	import "fmt"

	func main() {
		// get a mgo session	
		session, err := mgo.Dial("localhost")
		if err != nil {
			panic(err)
		}
		defer session.Close()
		session.SetMode(mgo.Monotonic, true)
		// nil options get initialized to gtm.DefaultOptions()
		ctx := gtm.Start(session, nil)
		// ctx.OpC is a channel to read ops from
		// ctx.ErrC is a channel to read errors from
		// ctx.Stop() stops all go routines started by gtm.Start
		for {
			// loop forever receiving events	
			select {
			case err= <-ctx.ErrC:
				// handle errors
				fmt.Println(err)
			case op:= <-ctx.OpC:
				// op will be an insert, delete, update, or drop to mongo
				// you can check which by calling 
				// op.IsInsert(), op.IsDelete(), op.IsUpdate(), or op.IsDrop()
				// op.Data will get you the full document for inserts and updates
				msg := fmt.Sprintf(`Got op <%v> for object <%v> 
				in database <%v>
				and collection <%v>
				and data <%v>
				and timestamp <%v>`,
					op.Operation, op.Id, op.GetDatabase(),
					op.GetCollection(), op.Data, op.Timestamp)
				fmt.Println(msg) // or do something more interesting
			}
		}
	}

### Configuration ###

	func NewUsers(op *gtm.Op) bool {
		return op.Namespace == "users.users" && op.IsInsert()
	}

	// if you want to listen only for certain events on certain collections
	// pass a filter function in options
	ctx := gtm.Start(session, &gtm.Options{
		Filter:              NewUsers, 	   // only receive inserts in the user collection
	})
	// more options are available for tuning
	ctx := gtm.Start(session, &gtm.Options{
		After:               nil,     	    // if nil defaults to LastOpTimestamp
		OpLogDatabaseName:   nil,     	    // defaults to "local"
		OpLogCollectionName: nil,     	    // defaults to a collection prefixed "oplog."
		CursorTimeout:       nil,     	    // defaults to 100s
		ChannelSize:         0,       	    // defaults to 20
		BufferSize:          25,            // defaults to 50. used to batch fetch documents on bursts of activity
		BufferDuration:      0,             // defaults to 750 ms. after this timeout the batch is force fetched
		WorkerCount:         8,             // defaults to 1. number of go routines batch fetching concurrently
		Ordering:            gtm.Document,  // defaults to gtm.Oplog. ordering guarantee of events on the output channel
		UpdateDataAsDelta:   false,         // set to true to only receive delta information in the Data field on updates (info straight from oplog)
		DirectReadNs: []string{"db.users"}, // set to a slice of namespaces to read data directly from bypassing the oplog
		DirectReadLimit:     200,           // defaults to 100. the maximum number of documents to return in each direct read query
	})

### Direct Reads ###

If, in addition to tailing the oplog, you would like to also read entire collections you can set the DirectReadNs field
to a slice of MongoDB namespaces.  Documents from these collections will be read directly and output on the ctx.OpC channel.  

You can wait till all the collections have been fully read by using the DirectReadWg wait group on the ctx.

	go func() {
		ctx.DirectReadWg.Wait()
		fmt.Println("direct reads are done")
	}()

### Advanced ###

You may want to distribute event handling between a set of worker processes on different machines.
To do this you can leverage the **github.com/rwynn/gtm/consistent** package.  

Create a TOML document containing a list of all the event handlers.

	Workers = [ "Tom", "Dick", "Harry" ] 

Create a consistent filter to distribute the work between Tom, Dick, and Harry.
	
	name := flag.String("name", "", "the name of this worker")
	flag.Parse()
	filter, filterErr := consistent.ConsistentHashFilterFromFile(*name, "/path/to/toml")
	if filterErr != nil {
		panic(filterErr)
	}

	// there is also a method **consistent.ConsistentHashFilterFromDocument** which allows
	// you to pass a Mongo document representing the config if you would like to avoid
	// copying the same config file to multiple servers

Pass the filter into the options when calling gtm.Tail

	ctx := gtm.Start(session, &gtm.Options{Filter: filter})

(Optional) If you have your own filter you can use the gtm utility method ChainOpFilters
	
	func ChainOpFilters(filters ...OpFilter) OpFilter
