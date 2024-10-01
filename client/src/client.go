package src

import (
	"baxos/common"
	"fmt"
	"strconv"
	"sync"
	"time"
)

/*
	This file defines the client struct and the new method that is invoked when creating a new client by the main
*/

type Client struct {
	id          int // unique client identifier as defined in the configuration
	numReplicas int // number of replicas

	replicaNodes []int

	incomingChan <-chan common.Message // used to collect responses
	outgoingChan chan<- common.Message // used to send requests

	writeRequestRatio float64 // ratio of write requests vs read requests

	messageCodes common.MessageCode
	logFilePath  string // the path to write the requests and responses time, used for sanity checks

	debugLevel int // current debug level

	testDuration int     // test duration in seconds
	arrivalRate  float64 // poisson rate of the arrivals (requests per second)

	arrivalTimeChan chan int64 // channel to which the poisson process adds new request arrival times in nanoseconds w.r.t test start time
	arrivalChan     chan bool  // channel to which the main scheduler adds new request indications, to be consumed by the request generation threads

	requests map[string]*ClientRequest // id of the request sent mapped to the time it was sent
	requestsMutex sync.RWMutex

	startTime time.Time // test start time

	clientListenAddress string // TCP address to which the client listens to new incoming TCP connections
	keyLen              int    // length of key
	valueLen            int    // length of value

	Finished bool
}

/*
	requestBatch contains a batch that was written to wire, and the time it was written
*/

const arrivalBufferSize = 1000000 // size of the buffer that collects new request arrivals

/*
	Instantiate a new Client instance, allocate the buffers
*/

func New(id int, logFilePath string, cfg *common.Config, incomingChan <-chan common.Message, outgoingChan chan<- common.Message) *Client {
	return &Client{
		id: id,

		incomingChan: incomingChan,
		outgoingChan: outgoingChan,

		logFilePath: logFilePath,

		debugLevel:        cfg.Flags.ClientFlags.DebugLevel,
		testDuration:      cfg.Flags.ClientFlags.TestDuration,
		arrivalRate:       cfg.Flags.ClientFlags.ArrivalRate,
		writeRequestRatio: cfg.Flags.ClientFlags.WriteRequestRatio,
		keyLen:            cfg.Flags.ClientFlags.KeyLength,
		valueLen:          cfg.Flags.ClientFlags.ValueLength,

		numReplicas:         len(cfg.Replicas),
		clientListenAddress: cfg.GetAddress(id),

		messageCodes: common.GetRPCCodes(),

		startTime: time.Time{},

		replicaNodes:    make([]int, 0),
		requests:        make(map[string]*ClientRequest),
		arrivalChan:     make(chan bool, arrivalBufferSize),
		arrivalTimeChan: make(chan int64, arrivalBufferSize),

		Finished: false,
	}
}

func (cl *Client) Init(cfg *common.Config) {
	// initialize replicaNodes
	for i := range cfg.Replicas {
		id, _ := strconv.ParseInt(cfg.Replicas[i].Id, 10, 32)
		cl.replicaNodes = append(cl.replicaNodes, int(id))
	}

	fmt.Printf("Initialized client %d \n", cl.id)
}

/*
	this is the main execution thread that listens to all the incoming messages
	It listens to incoming messages from the incomingChan, and invokes the appropriate handler depending on the message type
*/

func (cl *Client) Run() {
	for {
		cl.debug("Checking channel..", 0)
		replicaMessage := <-cl.incomingChan
		cl.debug("Received message", 0)

		switch replicaMessage.RpcPair.Code {
		case cl.messageCodes.WriteResponse:
			response := replicaMessage.RpcPair.Obj.(*common.WriteResponse)
			cl.debug(fmt.Sprintf("Client %d: Received write response from %d", cl.id, response.Sender), 0)
			cl.handleWriteResponse(response)

		case cl.messageCodes.ReadResponse:
			response := replicaMessage.RpcPair.Obj.(*common.ReadResponse)
			cl.debug(fmt.Sprintf("Client %d: Received read response from %d", cl.id, response.Sender), 0)
			cl.handleReadResponse(response)
		}
	}
}

func (cl *Client) debug(message string, level int) {
	if level >= cl.debugLevel {
		fmt.Printf("%v\n", message)
	}
}
