package src

import (
	"baxos/common"
	"fmt"
	"math/rand"
	"strconv"
)

type ReplicaNode struct {
	id     int32
	region string
}

/*
	defines the Replica struct and the new method that is invoked when creating a new replica
*/

type Replica struct {
	id          int32  // unique replica identifier as defined in the configuration file
	listenAddress string // TCP address to which the replica listens to new incoming TCP connections

	replicaNodes []ReplicaNode

	region string // region of the replica

	numReplicas int

	messageCodes common.MessageCode

	incomingChan <-chan common.Message     // used to collect ClientBatch messages for responses and Status messages for responses
	outgoingChan chan<- common.Message     // used to send ClientBatch messages to replicas

	logFilePath string // the path to write the log, used for sanity checks

	debugOn    bool // if turned on, the debug messages will be printed on the console
	debugLevel int  // current debug level

	serverStarted bool // to bootstrap

	baxosConsensus *Baxos // Baxos consensus data structs

	logPrinted bool // to check if log was printed before

	benchmarkMode int        // 0 for resident K/V store, 1 for redis

	incomingWriteRequests []*common.WriteRequest // incoming write requests
	incomingReadRequests  map[string]*ReadRequestInstance  // incoming read requests

	asynchronousReplicas   map[int][]int // for each time based epoch, the minority replicas that are attacked
	timeEpochSize          int           // how many ms for a given time epoch
}


/*
	instantiate a new replica instance, allocate the buffers
*/

func New(id int32, logFilePath string, debugOn bool, debugLevel int, benchmarkMode int, keyLen int, 
		 valLen int, timeEpochSize int, 
		 incomingChan <-chan common.Message, outgoingChan chan<- common.Message, region string) *Replica {
	return &Replica{
		id:         id,
		region: 	region,
		replicaNodes:  []ReplicaNode{},

		// rpcTable:     make(map[uint8]*common.RPCPair),
		messageCodes: common.GetRPCCodes(),

		incomingChan: incomingChan,
		outgoingChan: outgoingChan,

		logFilePath: logFilePath,

		debugOn:       debugOn,
		debugLevel:    debugLevel,
		serverStarted: false,

		logPrinted:       false,
		benchmarkMode:    benchmarkMode,
		incomingWriteRequests: make([]*common.WriteRequest, 0),
		incomingReadRequests:  make(map[string]*ReadRequestInstance),

		asynchronousReplicas:   make(map[int][]int),
		timeEpochSize:          timeEpochSize,
	}	
}

func (rp *Replica) Init(cfg *common.InstanceConfig, isAsync bool, roundTripTime int64) {
	rp.numReplicas = len(cfg.Replicas)
	rp.listenAddress = common.GetAddress(cfg.Replicas, rp.id)

	// initialize replicaNodes
	for i := 0; i < len(cfg.Replicas); i++ {
		int32Name, _ := strconv.ParseInt(cfg.Replicas[i].Id, 10, 32)
		rp.replicaNodes = append(rp.replicaNodes, ReplicaNode{id: int32(int32Name), region: cfg.Replicas[i].Region})
	}

	if isAsync {
		// initialize the attack replicas for each time epoch, we assume a total number of time of the run to be 10 minutes just for convenience, but this does not affect the correctness
		numEpochs := 10 * 60 * 1000 / rp.timeEpochSize
		s2 := rand.NewSource(39)
		r2 := rand.New(s2)

		for i := 0; i < numEpochs; i++ {
			rp.asynchronousReplicas[i] = []int{}
			for j := 0; j < rp.numReplicas/2; j++ {
				newReplica := r2.Intn(39)%rp.numReplicas + 1
				for rp.inArray(rp.asynchronousReplicas[i], newReplica) {
					newReplica = r2.Intn(39)%rp.numReplicas + 1
				}
				rp.asynchronousReplicas[i] = append(rp.asynchronousReplicas[i], newReplica)
			}
		}

		rp.debug(fmt.Sprintf("set of attacked nodes %v ", rp.asynchronousReplicas), -1)
	}

	rp.baxosConsensus = InitBaxosConsensus(rp, isAsync, roundTripTime)

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

			case rp.messageCodes.StatusRPC:
				statusMessage := replicaMessage.RpcPair.Obj.(*common.Status)
				rp.debug("Status message from "+fmt.Sprintf("%#v", statusMessage.Sender), 3)
				rp.handleStatus(statusMessage)

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

/*
	checks if replica is in ints
*/

func (rp *Replica) inArray(ints []int, replica int) bool {
	for i := 0; i < len(ints); i++ {
		if ints[i] == replica {
			return true
		}
	}
	return false
}

//debug printing
func (rp *Replica) debug(s string, i int) {
	if rp.debugOn && i >= rp.debugLevel {
		fmt.Print(s + "\n")
	}
}
