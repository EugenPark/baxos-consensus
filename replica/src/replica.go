package src

import (
	"baxos/common"
	"fmt"
	"strconv"
)

/*
	defines the Replica struct and the new method that is invoked when creating a new replica
*/

type Replica struct {
	id          int  // unique replica identifier as defined in the configuration file
	listenAddress string // TCP address to which the replica listens to new incoming TCP connections

	replicaNodes []int

	region string // region of the replica

	numReplicas int

	messageCodes common.MessageCode

	incomingChan <-chan common.Message     // used to collect ClientBatch messages for responses and Status messages for responses
	outgoingChan chan<- common.Message     // used to send ClientBatch messages to replicas

	logFilePath string // the path to write the log, used for sanity checks

	debugOn    bool // if turned on, the debug messages will be printed on the console
	debugLevel int  // current debug level

	baxosConsensus *Baxos // Baxos consensus data structs

	logPrinted bool // to check if log was printed before

	benchmarkMode int        // 0 for resident K/V store, 1 for redis

	incomingWriteRequests []*common.WriteRequest // incoming write requests
	incomingReadRequests  map[string]*ReadRequestInstance  // incoming read requests
}


/*
	instantiate a new replica instance, allocate the buffers
*/

func New(id int, logFilePath string, debugOn bool, debugLevel int, benchmarkMode int, keyLen int, 
		 valLen int, incomingChan <-chan common.Message, outgoingChan chan<- common.Message,
		 region string) *Replica {
	return &Replica{
		id:         id,
		region: 	region,
		replicaNodes:  make([]int, 0),

		messageCodes: common.GetRPCCodes(),

		incomingChan: incomingChan,
		outgoingChan: outgoingChan,

		logFilePath: logFilePath,

		debugOn:       debugOn,
		debugLevel:    debugLevel,

		logPrinted:       false,
		benchmarkMode:    benchmarkMode,
		incomingWriteRequests: make([]*common.WriteRequest, 0),
		incomingReadRequests:  make(map[string]*ReadRequestInstance),
	}	
}

func (rp *Replica) Init(cfg *common.InstanceConfig, roundTripTime int) {
	rp.numReplicas = len(cfg.Replicas)
	rp.listenAddress = common.GetAddress(cfg.Replicas, rp.id)

	// initialize replicaNodes
	for i := 0; i < len(cfg.Replicas); i++ {
		id, _ := strconv.ParseInt(cfg.Replicas[i].Id, 10, 32)
		rp.replicaNodes = append(rp.replicaNodes, int(id))
	}

	rp.baxosConsensus = InitBaxosConsensus(rp, roundTripTime)

	fmt.Printf("Initialized replica %v\n", rp.id)
}

/*
	this is the main execution thread that listens to all the incoming messages
	It listens to incoming messages from the incomingChan, and invokes the appropriate handler depending on the message type
*/

func (rp *Replica) Run() {
	for {
		select {
		case <-rp.baxosConsensus.wakeupChan:
			rp.proposeAfterBackingOff()
		case instance := <-rp.baxosConsensus.timeOutChan:
			rp.randomBackOff(instance)
		case replicaMessage := <-rp.incomingChan:
			switch replicaMessage.RpcPair.Code {
			case rp.messageCodes.PrintLog:
				rp.debug("Received Print Log request", 3)
				if !rp.logPrinted {
					rp.logPrinted = true
					rp.printBaxosLogConsensus() // this is for consensus testing purposes
					rp.debug("Printing Log", 3)
				}
			case rp.messageCodes.WriteRequest:
				writeRequest := replicaMessage.RpcPair.Obj.(*common.WriteRequest)
				rp.debug(fmt.Sprintf("Client write request message from %#v", writeRequest.Sender), 0)
				rp.handleWriteRequest(writeRequest)

			case rp.messageCodes.ReadRequest:
				readRequest := replicaMessage.RpcPair.Obj.(*common.ReadRequest)
				rp.debug(fmt.Sprintf("Client read request message from %#v", readRequest.Sender), 0)
				rp.handleReadRequest(readRequest)

			case rp.messageCodes.ReadPrepare:
				readPrepare := replicaMessage.RpcPair.Obj.(*common.ReadPrepare)
				rp.debug(fmt.Sprintf("Read prepare message from %#v", readPrepare.Sender), 0)
				rp.handleReadPrepare(readPrepare, replicaMessage.From)

			case rp.messageCodes.ReadPromise:
				readPromise := replicaMessage.RpcPair.Obj.(*common.ReadPromise)
				rp.debug(fmt.Sprintf("Read promise message from %#v", readPromise.Sender), 0)
				rp.handleReadPromise(readPromise)

			default:
				rp.debug("Baxos consensus message", 2)
				rp.handleBaxosConsensus(replicaMessage.RpcPair.Obj, replicaMessage.RpcPair.Code)
			}
		}
	}
}

//debug printing
func (rp *Replica) debug(s string, i int) {
	if rp.debugOn && i >= rp.debugLevel {
		fmt.Print(s + "\n")
	}
}
