package src

import (
	"baxos/common"
	"fmt"
	"math"
	"math/rand"
	"os"
)

type BaxosProposerInstance struct {
	preparedBallot        *common.Ballot // the ballot number for which the prepare message was sent
	numSuccessfulPromises int // the number of successful promise messages received

	highestSeenAcceptedBallot *common.Ballot       // the highest accepted ballot number among them set of Promise messages
	highestSeenAcceptedValue  *common.WriteRequest // the highest accepted value among the set of Promise messages

	proposedValue        *common.WriteRequest // the value that is proposed
	numSuccessfulAccepts int               // the number of successful accept messages received
}

type BaxosAcceptorInstance struct {
	promisedBallot *common.Ballot
	acceptedBallot *common.Ballot
	acceptedValue  *common.WriteRequest
}

/*
	instance defines the content of a single Baxos consensus instance
*/

type BaxosInstance struct {
	proposer_bookkeeping BaxosProposerInstance
	acceptor_bookkeeping BaxosAcceptorInstance
	decidedValue         *common.WriteRequest
	decided              bool
}

type ReadRequestInstance struct {
	responses []*common.ReadPromise
}

/*
	Baxos struct defines the replica wide consensus variables
*/

type Baxos struct {
	lastCommittedLogIndex int                   // the last log position that is committed
	replicatedLog         []BaxosInstance         // the replicated log of commands
	timer                 *common.TimerWithCancel // the timer for collecting promise / accept responses
	roundTripTime         int64                   // network round trip time in microseconds
	timeOutChan           chan int64              // to indicate that the timer has timed out

	isBackingOff bool                    // if the replica is backing off
	isProposing  bool                    // if the replica is proposing
	wakeupTimer  *common.TimerWithCancel // to wake up after backing off
	wakeupChan   chan bool               // to indicate that backoff period is completed
	retries      int                     // number of retries

	replica *Replica

	isAsync      bool // to simulate an asynchronous network

	quorumSize int
}

/*
	init Baxos Consensus data structs
*/

func InitBaxosConsensus(replica *Replica, isAsync bool, roundTripTime int64) *Baxos {

	replicatedLog := make([]BaxosInstance, 0)
	// create the genesis slot
	replicatedLog = append(replicatedLog, BaxosInstance {
		decidedValue: &common.WriteRequest{},
		decided:      true,
	})

	return &Baxos{
		lastCommittedLogIndex: 0,
		replicatedLog:         replicatedLog,
		timer:                 nil,
		roundTripTime:         roundTripTime,
		timeOutChan:           make(chan int64, 10000),
		isBackingOff:          false,
		isProposing:           false,
		wakeupTimer:           nil,
		wakeupChan:            make(chan bool, 10000),
		retries:               0,
		replica:               replica,
		isAsync:               isAsync,
		quorumSize:            int(replica.numReplicas/2 + 1),
	}
}

// calculate the backoff time for the proposer

func (rp *Replica) calculateBackOffTime() int64 {
	// k × 2^retries × 2 × RTT
	k := 1.0 - rand.Float64()
	rp.debug(fmt.Sprintf("Replica %d: k = %f, roundTripTime = %d, retries = %d", rp.id, k, rp.baxosConsensus.roundTripTime, rp.baxosConsensus.retries), 0)
	backoffTime := k * math.Pow(2, float64(rp.baxosConsensus.retries+1)) * float64(rp.baxosConsensus.roundTripTime)
	return int64(backoffTime)
}

// external API for Baxos messages

func (rp *Replica) handleBaxosConsensus(message common.Serializable, code uint8) {

	messageTmpl := "Instance %d: Received a message of type %s from %d"
	switch code {
	case rp.messageCodes.PrepareRequest:
		prepareRequest := message.(*common.PrepareRequest)
		msgType := "prepare"
		msg := fmt.Sprintf(messageTmpl, prepareRequest.InstanceNumber, msgType, prepareRequest.Sender)
		rp.debug(msg, 2)
		rp.handlePrepare(prepareRequest)

	case rp.messageCodes.PromiseReply:
		promiseReply := message.(*common.PromiseReply)
		msgType := "promise"
		msg := fmt.Sprintf(messageTmpl, promiseReply.InstanceNumber, msgType, promiseReply.Sender)
		rp.debug(msg, 2)
		
		rp.handlePromise(promiseReply)
	
	case rp.messageCodes.ProposeRequest: 
		proposeRequest := message.(*common.ProposeRequest)
		msgType := "propose"
		msg := fmt.Sprintf(messageTmpl, proposeRequest.InstanceNumber, msgType, proposeRequest.Sender)
		rp.debug(msg, 2)
		rp.handlePropose(proposeRequest)
	

	case rp.messageCodes.AcceptReply: 
		acceptReply := message.(*common.AcceptReply)
		msgType := "accept"
		msg := fmt.Sprintf(messageTmpl, acceptReply.InstanceNumber, msgType, acceptReply.Sender)
		rp.debug(msg, 2)
		rp.handleAccept(acceptReply)
	
	default:
		panic(fmt.Sprintf("Unknown message type %d", code))
	}
}

/*
	create instance number n
*/

func (rp *Replica) createInstance(n int) {

	if len(rp.baxosConsensus.replicatedLog) > n {
		// already exists
		return
	}

	numberOfNewInstances := n - len(rp.baxosConsensus.replicatedLog) + 1

	for i := 0; i < numberOfNewInstances; i++ {

		rp.baxosConsensus.replicatedLog = append(rp.baxosConsensus.replicatedLog, BaxosInstance {
			proposer_bookkeeping: BaxosProposerInstance {
				preparedBallot: &common.Ballot{
					Number:    -1,
					ReplicaId: int64(rp.id),
				},
				numSuccessfulPromises:     0,
				highestSeenAcceptedBallot: &common.Ballot{
					Number:    -1,
					ReplicaId: int64(rp.id),
				},
				highestSeenAcceptedValue:  &common.WriteRequest{},
				proposedValue:             &common.WriteRequest{},
				numSuccessfulAccepts:      0,
			},
			acceptor_bookkeeping: BaxosAcceptorInstance{
				promisedBallot: &common.Ballot{
					Number:    -1,
					ReplicaId: int64(rp.id),
				},
				acceptedBallot: &common.Ballot{
					Number:    -1,
					ReplicaId: int64(rp.id),
				},
				acceptedValue:  &common.WriteRequest{},
			},
			decidedValue: &common.WriteRequest{},
			decided:      false,
		})
	}
}

/*
	print the replicated log to check for log consistency
*/

func (rp *Replica) printBaxosLogConsensus() {
	f, err := os.Create(fmt.Sprintf("%s%d-consensus.txt", rp.logFilePath, rp.id))
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()

	for i := 0; i <= rp.baxosConsensus.lastCommittedLogIndex; i++ {
		if !rp.baxosConsensus.replicatedLog[i].decided {
			panic("should not happen")
		}
		if rp.baxosConsensus.replicatedLog[i].decidedValue.Command == nil {
			continue
		}
		f.WriteString(fmt.Sprintf("%d: %s\n", i, rp.baxosConsensus.replicatedLog[i].decidedValue.Command.Value))
	}
}

/*
	update SMR logic
*/

func (rp *Replica) updateSMR() {

	for i := rp.baxosConsensus.lastCommittedLogIndex + 1; i < len(rp.baxosConsensus.replicatedLog); i++ {

		baxosInstance := rp.baxosConsensus.replicatedLog[i]

		if !baxosInstance.decided {
			break
		}

		response := common.WriteResponse {
			UniqueId: baxosInstance.decidedValue.UniqueId,
			Success:  true,
			Sender:  int64(rp.id),
		}
		rp.baxosConsensus.lastCommittedLogIndex = i
		rp.sendClientResponse(&response, int(baxosInstance.decidedValue.Sender))
		rp.debug(fmt.Sprintf("Committed baxos consensus instance %d", i), 0)
	}
}
