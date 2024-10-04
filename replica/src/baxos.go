package src

import (
	"baxos/common"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"
)

const (
	PROCESSING_TIME = 40 * time.Millisecond
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
	proposerBookkeeping BaxosProposerInstance
	acceptorBookkeeping BaxosAcceptorInstance
	decidedValue         *common.WriteRequest
	decided              bool
	decidedAcksReceived  int
}

/*
	Baxos struct defines the replica wide consensus variables
*/

type BaxosMessage struct {
	message common.Serializable
	code    uint8
}

type Baxos struct {
	replicaId int

	lastCommittedLogIndex int                   // the last log position that is committed
	replicatedLog         []*BaxosInstance         // the replicated log of commands
	timer                 *common.TimerWithCancel // the timer for collecting promise / accept responses
	roundTripTime         time.Duration                   // network round trip time
	timeOutChan           chan int              // to indicate that the timer has timed out

	isBackingOff bool                    // if the replica is backing off
	isPreparing  bool                    // if the replica is proposing
	wakeupTimer  *common.TimerWithCancel // to wake up after backing off
	wakeupChan   chan bool               // to indicate that backoff period is completed
	retries      int   
	
	learnerChan chan *BaxosMessage 
	acceptorChan chan *BaxosMessage
	proposerChan chan *BaxosMessage
	readerChan chan *BaxosMessage

	quorumSize int
}

/*
	init Baxos Consensus data structs
*/

func InitBaxosConsensus(replicaId int, quorumSize int, roundTripTime int) *Baxos {

	genesisSlot := getInitialBaxosInstance(replicaId)
	genesisSlot.decided = true
	replicatedLog := []*BaxosInstance{genesisSlot}

	return &Baxos{
		replicaId:             replicaId,
		lastCommittedLogIndex: 0,
		replicatedLog:         replicatedLog,
		timer:                 nil,
		roundTripTime:         time.Duration(roundTripTime) * time.Millisecond,
		timeOutChan:           make(chan int, 10000),
		isBackingOff:          false,
		isPreparing:           false,
		wakeupTimer:           nil,
		wakeupChan:            make(chan bool, 10000),
		retries:               0,
		quorumSize:            quorumSize,
		learnerChan: make(chan *BaxosMessage, 10000),
		acceptorChan: make(chan *BaxosMessage, 10000),
		proposerChan: make(chan *BaxosMessage, 10000),
		readerChan: make(chan *BaxosMessage, 10000),
	}
}


func getInitialBaxosInstance(id int) *BaxosInstance {
	initialBallot := &common.Ballot{
		Number:    -1,
		ReplicaId: int64(id),
	}
	return &BaxosInstance {
		proposerBookkeeping: BaxosProposerInstance {
			preparedBallot: initialBallot,
			numSuccessfulPromises:     0,
			highestSeenAcceptedBallot: initialBallot,
			highestSeenAcceptedValue:  nil,
			proposedValue:             nil,
			numSuccessfulAccepts:      0,
		},
		acceptorBookkeeping: BaxosAcceptorInstance{
			promisedBallot: initialBallot,
			acceptedBallot: initialBallot,
			acceptedValue:  nil,
		},
		decidedValue: nil,
		decided:      false,
	}
}

// calculate the backoff time for the proposer

func (rp *Replica) calculateBackOffTime() time.Duration {
	// k × 2^retries × 2 × RTT + processing time
	k := 1.0 - rand.Float64()
	rp.debug(fmt.Sprintf("Replica %d: k = %f, roundTripTime = %d, retries = %d", rp.id, k, rp.baxosConsensus.roundTripTime, rp.baxosConsensus.retries), 0)
	multiplier := k * math.Pow(2, float64(rp.baxosConsensus.retries)) * 2
	return time.Duration(float64(rp.baxosConsensus.roundTripTime.Nanoseconds()) * multiplier) + PROCESSING_TIME
}

/*
	create instance number n
*/
func (rp *Replica) createInstance(n int) {
	baxos := rp.baxosConsensus

	if len(baxos.replicatedLog) > n {
		// already exists
		return
	}

	numberOfNewInstances := n - len(baxos.replicatedLog) + 1

	for i := 0; i < numberOfNewInstances; i++ {
		baxos.replicatedLog = append(baxos.replicatedLog, getInitialBaxosInstance(rp.id))
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

	// skip genesis slot 0 which is why it starts from 1
	for i := 1; i <= rp.baxosConsensus.lastCommittedLogIndex; i++ {
		if !rp.baxosConsensus.replicatedLog[i].decided || rp.baxosConsensus.replicatedLog[i].decidedValue.Command == nil {
			panic("should not happen")
		}
		f.WriteString(fmt.Sprintf("%d: %s\n", i, rp.baxosConsensus.replicatedLog[i].decidedValue.Command.Value))
	}
}

/*
	update SMR logic
*/

func (rp *Replica) updateSMR() {
	baxos := rp.baxosConsensus
	for i := range baxos.replicatedLog {
		baxosInstance := baxos.replicatedLog[i]

		// skip the genesis slot and not decided slot or already committed slot
		if !baxosInstance.decided || i == 0 || i <= baxos.lastCommittedLogIndex {
			continue
		}

		// remove already decided requests
		for j, writeRequest := range rp.incomingWriteRequests {
			if writeRequest.UniqueId == baxosInstance.decidedValue.UniqueId {
				// remove the request from the incoming write requests
				rp.incomingWriteRequests = append(rp.incomingWriteRequests[:j], rp.incomingWriteRequests[j+1:]...)
			}
		}

		baxos.lastCommittedLogIndex = i
		
		rp.debug(fmt.Sprintf("Send Client response for committed baxos consensus instance %d", i), 0)
	}
}
