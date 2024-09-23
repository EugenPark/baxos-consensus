package src

import (
	"baxos/common"
	"fmt"
	"strconv"
	"time"
)

type ReplicaNode struct {
	id     int32
	region string
}

type ClientRequest struct {
	start    time.Time
	end      time.Time
}

func (r *ClientRequest) StartTime() {
	r.start = time.Now()
}

func (r *ClientRequest) EndTime() {
	r.end = time.Now()
}

func (r *ClientRequest) Duration() time.Duration {
	return r.end.Sub(r.start)
}

/*
	This file defines the client struct and the new method that is invoked when creating a new client by the main
*/

type Client struct {
	id          int32 // unique client identifier as defined in the configuration
	numReplicas int   // number of replicas
	region	  	string

	replicaNodes []ReplicaNode

	incomingChan <-chan common.Message // used to collect ClientBatch messages for responses and Status messages for responses
	outgoingChan chan<- common.Message // used to send ClientBatch messages to replicas

	writeRequestRatio float64 // ratio of write requests vs read requests

	messageCodes common.MessageCode
	logFilePath  string // the path to write the requests and responses time, used for sanity checks

	debugOn    bool // if turned on, the debug messages will be printed on the console
	debugLevel int  // current debug level

	testDuration int // test duration in seconds
	arrivalRate  float64 // poisson rate of the arrivals (requests per second)

	arrivalTimeChan     chan int64               // channel to which the poisson process adds new request arrival times in nanoseconds w.r.t test start time
	arrivalChan         chan bool                // channel to which the main scheduler adds new request indications, to be consumed by the request generation threads
	RequestType         string                   // [request] for sending a stream of client requests, [status] for sending a status request
	OperationType       int                      // status operation type 1 (bootstrap server), 2: print log
	writeRequests  	map[string]*ClientRequest // id of the request sent mapped to the time it was sent
	readRequests   	map[string]*ClientRequest // id of the request sent mapped to the time it was sent
	startTime           time.Time                // test start time
	clientListenAddress string                   // TCP address to which the client listens to new incoming TCP connections
	keyLen              int                      // length of key
	valueLen            int                      // length of value

	finished           bool
}

/*
	requestBatch contains a batch that was written to wire, and the time it was written
*/

const statusTimeout = 5               // time to wait for a status request in seconds

const arrivalBufferSize = 1000000     // size of the buffer that collects new request arrivals

/*
	Instantiate a new Client instance, allocate the buffers
*/

func New(id int32, logFilePath string, testDuration int, arrivalRate float64, requestType string, writeRequestRatio float64,
	     operationType int, debugOn bool, debugLevel int, keyLen int, valLen int,
		 incomingChan <-chan common.Message, outgoingChan chan<- common.Message, region string) *Client {
	return &Client{
		id:              id,
		region:          region,
		replicaNodes:    []ReplicaNode{},
		incomingChan:    incomingChan,
		outgoingChan:    outgoingChan,
		messageCodes:    common.GetRPCCodes(),
		logFilePath:     logFilePath,

		debugOn:    debugOn,
		debugLevel: debugLevel,

		testDuration:        testDuration,
		arrivalRate:         arrivalRate,
		arrivalTimeChan:     make(chan int64, arrivalBufferSize),
		arrivalChan:         make(chan bool, arrivalBufferSize),
		RequestType:         requestType,
		writeRequestRatio:   writeRequestRatio,
		OperationType:       operationType,
		writeRequests:   make(map[string]*ClientRequest),
		readRequests:    make(map[string]*ClientRequest),
		startTime:           time.Time{},
		keyLen:              keyLen,
		valueLen:            valLen,
		finished:            false,
	}
}

func (cl *Client) Init(cfg *common.InstanceConfig) {
	cl.numReplicas = len(cfg.Replicas)
	cl.clientListenAddress = common.GetAddress(cfg.Clients, cl.id)

	// initialize replicaNodes
	for i := 0; i < len(cfg.Replicas); i++ {
		int32Name, _ := strconv.ParseInt(cfg.Replicas[i].Id, 10, 32)
		cl.replicaNodes = append(cl.replicaNodes, ReplicaNode{id: int32(int32Name), region: cfg.Replicas[i].Region})
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

		case cl.messageCodes.StatusRPC:
			clientStatusResponse := replicaMessage.RpcPair.Obj.(*common.Status)
			cl.debug(fmt.Sprintf("Client status  %#v", clientStatusResponse), 3)
			cl.handleClientStatusResponse(clientStatusResponse)
		}
	}
}

func (cl *Client) debug(message string, level int) {
	if cl.debugOn && level >= cl.debugLevel {
		fmt.Printf("%v\n", message)
	}
}
