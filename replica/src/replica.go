package src

import (
	"baxos/common"
	"fmt"
	"strconv"
	"time"
)

/*
	defines the Replica struct and the new method that is invoked when creating a new replica
*/

type Replica struct {
	id          int  // unique replica identifier as defined in the configuration file
	listenAddress string // TCP address to which the replica listens to new incoming TCP connections

	replicaNodes []int

	numReplicas int

	messageCodes common.MessageCode

	incomingChan <-chan common.Message     // used to collect ClientBatch messages for responses and Status messages for responses
	outgoingChan chan<- common.Message     // used to send ClientBatch messages to replicas

	logFilePath string // the path to write the log, used for sanity checks

	debugLevel int  // current debug level

	baxosConsensus *Baxos // Baxos consensus data structs

	logPrinted bool // to check if log was printed before

	benchmarkMode int        // 0 for resident K/V store, 1 for redis

	incomingWriteRequests []*common.WriteRequest // incoming write requests
}


/*
	instantiate a new replica instance, allocate the buffers
*/

func New(id int, logFilePath string, cfg *common.Config, incomingChan <-chan common.Message, outgoingChan chan<- common.Message) *Replica {
	return &Replica{
		id:         id,

		logFilePath: logFilePath,

		debugLevel:    cfg.Flags.ReplicaFlags.DebugLevel,
		benchmarkMode:    cfg.Flags.ReplicaFlags.BenchmarkMode,
		listenAddress: cfg.GetAddress(id),
		numReplicas: len(cfg.Replicas),

		incomingChan: incomingChan,
		outgoingChan: outgoingChan,

		messageCodes: common.GetRPCCodes(),
		logPrinted:       false,

		replicaNodes:  make([]int, 0),
		incomingWriteRequests: make([]*common.WriteRequest, 0),
	}	
}

func (rp *Replica) Init(cfg *common.Config) {
	// initialize replicaNodes
	for i := 0; i < len(cfg.Replicas); i++ {
		id, _ := strconv.ParseInt(cfg.Replicas[i].Id, 10, 32)
		rp.replicaNodes = append(rp.replicaNodes, int(id))
	}

	rp.baxosConsensus = InitBaxosConsensus(rp.id, int(rp.numReplicas/2 + 1), cfg.Flags.ReplicaFlags.RoundTripTime)

	fmt.Printf("Initialized replica %v\n", rp.id)
}

/*
	this is the main execution thread that listens to all the incoming messages
	It listens to incoming messages from the incomingChan, and invokes the appropriate handler depending on the message type
*/

func (rp *Replica) Run() {
	go rp.runBaxosAcceptor()
	go rp.runBaxosLearner()
	go rp.runBaxosProposer()
	go rp.runBaxosReader()
	go func() {
		for {
			if len(rp.incomingWriteRequests) > 0 {
				rp.tryPrepare()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	for {
		select {
		case <-rp.baxosConsensus.wakeupChan:
			rp.prepareAfterBackoff()
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
			
			case rp.messageCodes.PrepareRequest:
				rp.baxosConsensus.acceptorChan <- &BaxosMessage{
					message: replicaMessage.RpcPair.Obj.(*common.PrepareRequest),
					code:   rp.messageCodes.PrepareRequest,
				}
				
			case rp.messageCodes.PromiseReply:
				rp.baxosConsensus.proposerChan <- &BaxosMessage{
					message: replicaMessage.RpcPair.Obj.(*common.PromiseReply),
					code:   rp.messageCodes.PromiseReply,
				}
						
			case rp.messageCodes.ProposeRequest: 
				rp.baxosConsensus.acceptorChan <- &BaxosMessage{
					message: replicaMessage.RpcPair.Obj.(*common.ProposeRequest),
					code:   rp.messageCodes.ProposeRequest,
				}
					
			case rp.messageCodes.AcceptReply: 
				rp.baxosConsensus.proposerChan <- &BaxosMessage{
					message: replicaMessage.RpcPair.Obj.(*common.AcceptReply),
					code:   rp.messageCodes.AcceptReply,
				}
				
			case rp.messageCodes.DecideInfo:
				rp.baxosConsensus.learnerChan <- &BaxosMessage{
					message: replicaMessage.RpcPair.Obj.(*common.DecideInfo),
					code:   rp.messageCodes.DecideInfo,
				}
				
			case rp.messageCodes.ReadRequest:
				rp.baxosConsensus.readerChan <- &BaxosMessage{
					message: replicaMessage.RpcPair.Obj.(*common.ReadRequest),
					code:   rp.messageCodes.ReadRequest,
				}

			case rp.messageCodes.RinseRequest:
				rp.baxosConsensus.readerChan <- &BaxosMessage{
					message: replicaMessage.RpcPair.Obj.(*common.RinseRequest),
					code:   rp.messageCodes.RinseRequest,
				}
			}
		}
	}
}

//debug printing
func (rp *Replica) debug(s string, i int) {
	if i >= rp.debugLevel {
		fmt.Print(s + "\n")
	}
}
