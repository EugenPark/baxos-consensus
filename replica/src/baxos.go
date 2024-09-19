package src

import (
	"baxos/common"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
)

type BaxosProposerInstance struct {
	preparedBallot        *common.Ballot // the ballot number for which the prepare message was sent
	numSuccessfulPromises int32 // the number of successful promise messages received

	highestSeenAcceptedBallot *common.Ballot       // the highest accepted ballot number among them set of Promise messages
	highestSeenAcceptedValue  *common.ReplicaBatch // the highest accepted value among the set of Promise messages

	proposedValue        *common.ReplicaBatch // the value that is proposed
	numSuccessfulAccepts int32               // the number of successful accept messages received
}

type BaxosAcceptorInstance struct {
	promisedBallot *common.Ballot
	acceptedBallot *common.Ballot
	acceptedValue  *common.ReplicaBatch
}

/*
	instance defines the content of a single Baxos consensus instance
*/

type BaxosInstance struct {
	proposer_bookkeeping BaxosProposerInstance
	acceptor_bookkeeping BaxosAcceptorInstance
	decidedValue         *common.ReplicaBatch
	decided              bool
}

/*
	Baxos struct defines the replica wide consensus variables
*/

type Baxos struct {
	lastCommittedLogIndex int32                   // the last log position that is committed
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

	quorumSize int32
}

/*
	init Baxos Consensus data structs
*/

func InitBaxosConsensus(replica *Replica, isAsync bool, roundTripTime int64) *Baxos {

	replicatedLog := make([]BaxosInstance, 0)
	// create the genesis slot
	replicatedLog = append(replicatedLog, BaxosInstance {
		decidedValue: &common.ReplicaBatch{},
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
		quorumSize:            int32(replica.numReplicas/2 + 1),
	}
}

// calculate the backoff time for the proposer

func (rp *Replica) calculateBackOffTime() int64 {
	// k × 2^retries × 2 × RTT
	k := 1.0 - rand.Float64()
	rp.debug(fmt.Sprintf("Replica %d: k = %f, roundTripTime = %d, retries = %d", rp.id, k, rp.baxosConsensus.roundTripTime, rp.baxosConsensus.retries ), 0)
	backoffTime := k * math.Pow(2, float64(rp.baxosConsensus.retries+1)) * float64(rp.baxosConsensus.roundTripTime)
	return int64(backoffTime)
}

// external API for Baxos messages

func (rp *Replica) handleBaxosConsensus(message common.Serializable, code uint8) {

	messageTmpl := "Instance %d, ballot (%d, %d): Received a message of type %s from %d"
	switch code {
	case rp.messageCodes.PrepareRequest:
		prepareRequest := message.(*common.PrepareRequest)
		msgType := "prepare"
		msg := fmt.Sprintf(messageTmpl, prepareRequest.InstanceNumber, prepareRequest.PrepareBallot.ReplicaId, prepareRequest.PrepareBallot.Number, msgType, prepareRequest.Sender)
		rp.debug(msg, 2)
		rp.handlePrepare(prepareRequest)

	case rp.messageCodes.PromiseReply:
		promiseReply := message.(*common.PromiseReply)
		// msgType := "promise"
		// msg := fmt.Sprintf(messageTmpl, (*promiseReply).InstanceNumber, (*promiseReply).LastPromisedBallot.ReplicaId, (*promiseReply).LastPromisedBallot.Number, msgType, (*promiseReply).Sender)
		// rp.debug(msg, 2)
		
		rp.handlePromise(promiseReply)
	
	case rp.messageCodes.ProposeRequest: 
		proposeRequest := message.(*common.ProposeRequest)
		// msgType := "propose"
		// msg := fmt.Sprintf(messageTmpl, proposeRequest.InstanceNumber, proposeRequest.ProposeBallot.ReplicaId, proposeRequest.ProposeBallot.Number, msgType, proposeRequest.Sender)
		// rp.debug(msg, 2)
		rp.handlePropose(proposeRequest)
	

	case rp.messageCodes.AcceptReply: 
		acceptReply := message.(*common.AcceptReply)
		// msgType := "accept"
		// msg := fmt.Sprintf(messageTmpl, acceptReply.InstanceNumber, acceptReply.AcceptBallot.ReplicaId, acceptReply.AcceptBallot.Number, msgType, acceptReply.Sender)
		// rp.debug(msg, 2)
		rp.handleAccept(acceptReply)
	
	default:
		panic("Unknown message type")
	
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
					ReplicaId: rp.id,
				},
				numSuccessfulPromises:     0,
				highestSeenAcceptedBallot: &common.Ballot{
					Number:    -1,
					ReplicaId: rp.id,
				},
				highestSeenAcceptedValue:  &common.ReplicaBatch{},
				proposedValue:             &common.ReplicaBatch{},
				numSuccessfulAccepts:      0,
			},
			acceptor_bookkeeping: BaxosAcceptorInstance{
				promisedBallot: &common.Ballot{
					Number:    -1,
					ReplicaId: rp.id,
				},
				acceptedBallot: &common.Ballot{
					Number:    -1,
					ReplicaId: rp.id,
				},
				acceptedValue:  &common.ReplicaBatch{},
			},
			decidedValue: &common.ReplicaBatch{},
			decided:      false,
		})
	}
}

/*
	print the replicated log to check for log consistency
*/

func (rp *Replica) printBaxosLogConsensus() {
	f, err := os.Create(rp.logFilePath + strconv.Itoa(int(rp.id)) + "-consensus.txt")
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()

	for i := int32(0); i <= rp.baxosConsensus.lastCommittedLogIndex; i++ {
		if !rp.baxosConsensus.replicatedLog[i].decided {
			panic("should not happen")
		}
		for j := 0; j < len(rp.baxosConsensus.replicatedLog[i].decidedValue.Requests); j++ {
			for k := 0; k < len(rp.baxosConsensus.replicatedLog[i].decidedValue.Requests[j].Requests); k++ {
				_, _ = f.WriteString(strconv.Itoa(int(i)) + "-" + strconv.Itoa(j) + "-" + strconv.Itoa(k) + ":" + rp.baxosConsensus.replicatedLog[i].decidedValue.Requests[j].Requests[k].Command + "\n")
			}
		}
	}
}

/*
	update SMR logic
*/

func (rp *Replica) updateSMR() {

	for i := rp.baxosConsensus.lastCommittedLogIndex + 1; i < int32(len(rp.baxosConsensus.replicatedLog)); i++ {

		if rp.baxosConsensus.replicatedLog[i].decided {
			var clientResponses []*common.ClientBatch = rp.updateApplicationLogic(rp.baxosConsensus.replicatedLog[i].decidedValue.Requests)
			rp.sendClientResponses(clientResponses)
			if rp.debugOn {
				rp.debug("Committed baxos consensus instance "+"."+strconv.Itoa(int(i)), 0)
			}
			rp.baxosConsensus.lastCommittedLogIndex = i
		} else {
			break
		}
	}
}
