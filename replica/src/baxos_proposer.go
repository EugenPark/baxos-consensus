package src

import (
	"baxos/common"
	"fmt"
	"math"
	"time"
)

/*
	sets a timer, which once timeout will send an internal notification for setting the backoff timer
*/

func (rp *Replica) setTimer(instance int64) {
	rp.baxosConsensus.timer = common.NewTimerWithCancel(time.Duration(2 * rp.baxosConsensus.roundTripTime) * time.Microsecond)

	rp.baxosConsensus.timer.SetTimeoutFunction(func() {
		rp.baxosConsensus.timeOutChan <- instance

	})
	rp.baxosConsensus.timer.Start()
}

// this is triggered when the proposer timer timeout after waiting for promise / accept messages

func (rp *Replica) randomBackOff(instance int64) {
	rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Timed out", instance), 2)

	if rp.baxosConsensus.replicatedLog[instance].decided && rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.numSuccessfulAccepts >= rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: already decided, hence ignoring the timeout indication", instance), 2)
		return
	}

	rp.baxosConsensus.isBackingOff = true
	rp.baxosConsensus.isProposing = true
	rp.baxosConsensus.timer = nil
	rp.baxosConsensus.retries++

	// prepend the value
	rp.incomingWriteRequests = append([]*common.WriteRequest{rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.proposedValue}, rp.incomingWriteRequests...)

	rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.numSuccessfulPromises = 0
	rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.highestSeenAcceptedValue = &common.WriteRequest{}

	rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.proposedValue = &common.WriteRequest{}
	rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.numSuccessfulAccepts = 0

	rp.debug(fmt.Sprintf("PROPOSER: Instance %d: reset as a result of proposer bookkeeping after timeout", instance), 1)

	// set the backing off timer
	backoffTime := rp.calculateBackOffTime()

	rp.debug(fmt.Sprintf("PROPOSER: Backing off for %d microseconds", backoffTime), 2)
	
	rp.baxosConsensus.wakeupTimer = common.NewTimerWithCancel(time.Duration(backoffTime) * time.Microsecond)

	rp.baxosConsensus.wakeupTimer.SetTimeoutFunction(func() {
		rp.baxosConsensus.wakeupChan <- true
		rp.debug("PROPOSER: Finished backing off", 2)
		
	})
	rp.baxosConsensus.wakeupTimer.Start()
}

// this is triggered after the backoff timer timeouts and the proposer is ready to propose again

func (rp *Replica) proposeAfterBackingOff() {
	rp.debug("PROPOSER: Proposing after backing off", 2)
	
	rp.baxosConsensus.isBackingOff = false
	rp.sendPrepare()
}

/*
	send a prepare message to lastCommittedIndex + 1
*/

func (rp *Replica) sendPrepare() {

	nextFreeInstance := rp.baxosConsensus.lastCommittedLogIndex + 1
	rp.createInstance(int(nextFreeInstance))

	prevHighestAcceptedBallot := rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.highestSeenAcceptedBallot
	prevBallot := rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.preparedBallot

	rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.preparedBallot = 
		&common.Ballot {
			Number: int64(math.Max(float64(prevBallot.Number), float64(prevHighestAcceptedBallot.Number))) + 1,
			ReplicaId:     int64(rp.id),
		}
	
	rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.numSuccessfulPromises = 0
	rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.highestSeenAcceptedBallot = &common.Ballot {
		Number: -1,
		ReplicaId:     int64(rp.id),
	}
	rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.highestSeenAcceptedValue = &common.WriteRequest{}
	rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.proposedValue = &common.WriteRequest{}
	rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.numSuccessfulAccepts = 0

	for _, replicaId := range rp.replicaNodes {
		prepareMessage := common.PrepareRequest {
			InstanceNumber: int64(nextFreeInstance),
			PrepareBallot:  rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.preparedBallot,
			Sender:         int64(rp.id),
		}
		rp.outgoingChan <- common.Message {
			From:    rp.id,
			To:      replicaId,
			RpcPair: &common.RPCPair{Code: rp.messageCodes.PrepareRequest, Obj: &prepareMessage},
		}
	}

	rp.debug(fmt.Sprintf("PROPOSER: Instance %d, Ballot (%d, %d): Sent prepare", nextFreeInstance,
		rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.preparedBallot.ReplicaId,
		rp.baxosConsensus.replicatedLog[nextFreeInstance].proposer_bookkeeping.preparedBallot.Number), 1)
	
	if rp.baxosConsensus.timer != nil {
		rp.baxosConsensus.timer.Cancel()
	}
	rp.setTimer(int64(nextFreeInstance))
}

/*
	Handler for promise message
*/

func (rp *Replica) handlePromise(message *common.PromiseReply) {

	// rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Received a promise with last Promised Ballot (%d, %d)", message.InstanceNumber, message.LastPromisedBallot.ReplicaId, message.LastPromisedBallot.Number), 1)

	if rp.baxosConsensus.replicatedLog[message.InstanceNumber].decided {

		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already decided, hence ignoring the promise", message.InstanceNumber), 1)
		
		return
	}

	if message.Decided {
		if !rp.baxosConsensus.replicatedLog[message.InstanceNumber].decided {
			rp.baxosConsensus.replicatedLog[message.InstanceNumber].decided = true
			rp.baxosConsensus.replicatedLog[message.InstanceNumber].decidedValue = message.DecidedValue
			rp.debug(fmt.Sprintf("PROPOSER: Instance %d: decided using promise response, hence setting the decided value", message.InstanceNumber), 2)
			
			rp.updateSMR()
			return
		}
	}

	if rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.numSuccessfulPromises >= rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already enough promises, hence ignoring the promise", message.InstanceNumber), 1)
		return
	}

	if message.Promise && !message.LastPromisedBallot.IsGreaterThan(rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.preparedBallot) {
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.numSuccessfulPromises++
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Received a promise, hence incrementing the promise count", message.InstanceNumber), 1)

		if message.LastAcceptedBallot.IsGreaterThan(rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.highestSeenAcceptedBallot) {
			rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.highestSeenAcceptedBallot = message.LastAcceptedBallot
			rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.highestSeenAcceptedValue = message.LastAcceptedValue
		}
		if rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.numSuccessfulPromises == rp.baxosConsensus.quorumSize {
			rp.debug(fmt.Sprintf("PROPOSER: Instance %d: received quorum of promises, hence proposing", message.InstanceNumber), 2)
			
			rp.sendPropose(int(message.InstanceNumber))
		}
	}

}

// invoked upon receiving a client batch
func (rp *Replica) tryPropose() {

	if rp.baxosConsensus.isProposing || rp.baxosConsensus.isBackingOff {
		rp.debug("PROPOSER: Already proposing or backing off, hence ignoring the propose request", 1)
		
		return
	}

	rp.sendPrepare()
	rp.baxosConsensus.isProposing = true
}

func (rp *Replica) sendReadPrepare(request *common.ReadRequest) {
	for _, replicaId := range rp.replicaNodes {
		rp.outgoingChan <- common.Message {
			From:    rp.id,
			To:      replicaId,
			RpcPair: &common.RPCPair{Code: rp.messageCodes.ReadPrepare, Obj: &common.ReadPrepare{
				UniqueId: request.UniqueId,
				Sender:  request.Sender,
			}},
		}
	}
}

func (rp *Replica) handleReadPromise(message *common.ReadPromise) {
	uniqueId := message.UniqueId
	rp.debug(fmt.Sprintf("READER: UniqueId %s: Received a promise from %d", message.UniqueId, message.Sender), 1)


	if len(rp.incomingReadRequests[uniqueId].responses) >= rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("READER: UniqueId %s: Already received enough promises, hence ignoring the promise", uniqueId), 1)
		return
	}

	rp.incomingReadRequests[uniqueId].responses = append(rp.incomingReadRequests[uniqueId].responses, message)
	if len(rp.incomingReadRequests[uniqueId].responses) == rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("READER: UniqueId %s: Received quorum of promises, hence sending the read response", uniqueId), 1)
		rp.sendClientReadResponse(uniqueId, int(message.Sender))
	}
}
/*
	propose a command for instance
*/

func (rp *Replica) sendPropose(instance int) {

	rp.createInstance(int(instance))

	if !rp.baxosConsensus.replicatedLog[instance-1].decided {
		panic("error, previous index not decided")
	}

	decideInfo := common.DecideInfo {
		InstanceNumber: int64(instance - 1),
		DecidedValue:   rp.baxosConsensus.replicatedLog[instance-1].decidedValue,
	}

	// propose message
	var proposeValue *common.WriteRequest

	if rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.highestSeenAcceptedBallot.Number != -1 {
		proposeValue = rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.highestSeenAcceptedValue
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Highest seen accepted value is proposed %v", instance, proposeValue), 1)
	} else if len(rp.incomingWriteRequests) > 0 {
		proposeValue, rp.incomingWriteRequests = rp.incomingWriteRequests[0], rp.incomingWriteRequests[1:]
	} else {
		proposeValue = &common.WriteRequest{}
	}

	if proposeValue.UniqueId == "" {
		proposeValue.Sender = -1
	}

	rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.proposedValue = proposeValue

	for _, replicaId := range rp.replicaNodes {
		proposeRequest := common.ProposeRequest {
			InstanceNumber: int64(instance),
			ProposeBallot:  rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.preparedBallot,
			ProposeValue:   proposeValue,
			Sender:         int64(rp.id),
			DecideInfo:     &decideInfo,
		}
		rp.outgoingChan <- common.Message{
			From:    rp.id,
			To:      replicaId,
			RpcPair: &common.RPCPair{Code: rp.messageCodes.ProposeRequest, Obj: &proposeRequest},
		}
	}
	rp.debug(fmt.Sprintf("PROPOSER: Instance %d, Ballot (%d, %d): Broadcast propose", instance,
		rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.preparedBallot.ReplicaId,
		rp.baxosConsensus.replicatedLog[instance].proposer_bookkeeping.preparedBallot.Number), 2)
	

	if rp.baxosConsensus.timer != nil {
		rp.baxosConsensus.timer.Cancel()
	}
	rp.setTimer(int64(instance))
}

/*
	handler for accept messages
*/

func (rp *Replica) handleAccept(message *common.AcceptReply) {

	if rp.baxosConsensus.replicatedLog[message.InstanceNumber].decided {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already decided, hence ignoring the accept", message.InstanceNumber), 1)
		return
	}

	if message.Decided {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: decided using accept response, hence setting the decided value", message.InstanceNumber), 2)
		
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].decided = true
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].decidedValue = message.DecidedValue
		rp.updateSMR()
		return
	}

	if rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.numSuccessfulAccepts >= rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already enough accepts, hence ignoring the accept", message.InstanceNumber), 2)
		return
	}

	if message.Accept && message.AcceptBallot.IsEqualTo(rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.preparedBallot) {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d, Ballot (%d, %d): Received an accept, hence incrementing the accept count",
			message.InstanceNumber,
			message.AcceptBallot.ReplicaId,
			message.AcceptBallot.Number), 2)
		
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.numSuccessfulAccepts++
	}

	if rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.numSuccessfulAccepts >= rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Received quorum of accepts, hence deciding", message.InstanceNumber), 2)
		
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].decided = true
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].decidedValue = rp.baxosConsensus.replicatedLog[message.InstanceNumber].proposer_bookkeeping.proposedValue
		rp.updateSMR()

		rp.baxosConsensus.isProposing = false
		rp.baxosConsensus.retries--
		if rp.baxosConsensus.retries < 0 {
			rp.baxosConsensus.retries = 0
		}
		if rp.baxosConsensus.timer != nil {
			rp.baxosConsensus.timer.Cancel()
		}
		rp.baxosConsensus.timer = nil

		if rp.baxosConsensus.wakeupTimer != nil {
			rp.baxosConsensus.wakeupTimer.Cancel()
		}
		rp.baxosConsensus.wakeupTimer = nil
		rp.baxosConsensus.isBackingOff = false
	}
}
