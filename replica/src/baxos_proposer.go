package src

import (
	"baxos/common"
	"fmt"
)

/*
	sets a timer, which once timeout will send an internal notification for setting the backoff timer
*/

func (rp *Replica) setTimer(instance int) {
	baxos := rp.baxosConsensus
	baxos.timer = common.NewTimerWithCancel(baxos.roundTripTime * 2 + PROCESSING_TIME) // 2 * RTT

	baxos.timer.SetTimeoutFunction(func() {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Timeout", instance), 3)
		baxos.timeOutChan <- instance
	})
	baxos.timer.Start()
}

// this is triggered when the proposer timer timeout after waiting for promise / accept messages

func (rp *Replica) randomBackOff(instance int) {
	baxos := rp.baxosConsensus
	baxosInstance := baxos.replicatedLog[instance]

	if baxosInstance.decided {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: already decided, hence ignoring the timeout indication", instance), 2)
		return
	}

	baxos.isBackingOff = true
	baxos.isPreparing = true
	baxos.timer = nil
	baxos.retries++

	// reset the proposer bookkeeping
	baxosInstance.proposerBookkeeping.numSuccessfulPromises = 0
	baxosInstance.proposerBookkeeping.numSuccessfulAccepts = 0
	baxosInstance.proposerBookkeeping.proposedValue = nil

	rp.debug(fmt.Sprintf("PROPOSER: Instance %d: reset as a result of proposer bookkeeping after timeout", instance), 0)

	// set the backing off timer
	backoffTime := rp.calculateBackOffTime()
	
	baxos.wakeupTimer = common.NewTimerWithCancel(backoffTime)
	rp.debug(fmt.Sprintf("PROPOSER: Backing off for %d milliseconds", backoffTime.Milliseconds()), 3)


	rp.baxosConsensus.wakeupTimer.SetTimeoutFunction(func() {
		rp.baxosConsensus.wakeupChan <- true
		rp.debug("PROPOSER: Finished backing off", 3)
		
	})
	rp.baxosConsensus.wakeupTimer.Start()
}

// this is triggered after the backoff timer timeouts and the proposer is ready to propose again

func (rp *Replica) prepareAfterBackoff() {
	rp.debug("PROPOSER: Preparing after backing off", 3)
	
	rp.baxosConsensus.isBackingOff = false
	rp.sendPrepare()
}

/*
	send a prepare message to lastCommittedIndex + 1
*/

func (rp *Replica) sendPrepare() {
	baxos := rp.baxosConsensus
	nextFreeInstance := baxos.lastCommittedLogIndex + 1
	rp.createInstance(nextFreeInstance)

	baxosInstance := baxos.replicatedLog[nextFreeInstance]
	baxosInstance.proposerBookkeeping.preparedBallot.Number++

	for _, replicaId := range rp.replicaNodes {
		prepareMessage := common.PrepareRequest {
			InstanceNumber: int64(nextFreeInstance),
			PrepareBallot:  baxosInstance.proposerBookkeeping.preparedBallot,
			Sender:         int64(rp.id),
		}
		rp.outgoingChan <- common.Message {
			From:    rp.id,
			To:      replicaId,
			RpcPair: &common.RPCPair{Code: rp.messageCodes.PrepareRequest, Obj: &prepareMessage},
		}
	}

	rp.debug(fmt.Sprintf("PROPOSER: Instance %d, Ballot (%d, %d): Sent prepare", nextFreeInstance,
		baxosInstance.proposerBookkeeping.preparedBallot.ReplicaId,
		baxosInstance.proposerBookkeeping.preparedBallot.Number), 1)
	
	if rp.baxosConsensus.timer != nil {
		rp.baxosConsensus.timer.Cancel()
	}
	rp.setTimer(nextFreeInstance)
}

/*
	Handler for promise message
*/

func (rp *Replica) handlePromise(message *common.PromiseReply) {
	baxos := rp.baxosConsensus
	if baxos.isBackingOff {
		rp.debug("PROPOSER: Already backing off, hence ignoring the promise", 3)
		return
	}
	
	baxosInstance := baxos.replicatedLog[message.InstanceNumber]
	if baxosInstance.decided {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already decided, hence ignoring the promise", message.InstanceNumber), 3)
		return
	}

	if !message.Promise {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Promise rejected, hence ignoring promise", message.InstanceNumber), 3)
		return
	}

	if baxosInstance.proposerBookkeeping.numSuccessfulPromises >= rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already enough promises, hence ignoring the promise", message.InstanceNumber), 3)
		return
	}

	baxosInstance.proposerBookkeeping.numSuccessfulPromises++
	rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Received a promise, hence incrementing the promise count", message.InstanceNumber), 3)

	if message.LastAcceptedBallot.IsGreaterThan(baxosInstance.proposerBookkeeping.highestSeenAcceptedBallot) {
		baxosInstance.proposerBookkeeping.highestSeenAcceptedBallot = message.LastAcceptedBallot
		baxosInstance.proposerBookkeeping.highestSeenAcceptedValue = message.LastAcceptedValue
	}

	if baxosInstance.proposerBookkeeping.numSuccessfulPromises == baxos.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: received quorum of promises, hence proposing", message.InstanceNumber), 3)
		rp.sendPropose(int(message.InstanceNumber))
	}
}

// invoked upon receiving a client batch
func (rp *Replica) tryPrepare() {
	if rp.baxosConsensus.isPreparing || rp.baxosConsensus.isBackingOff {
		rp.debug("PROPOSER: Already proposing or backing off, hence ignoring the propose request", 1)
		
		return
	}

	rp.sendPrepare()
	rp.baxosConsensus.isPreparing = true
}
/*
	propose a command for instance n
*/

func (rp *Replica) sendPropose(instance int) {
	rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Proposing", instance), 3)
	
	baxos := rp.baxosConsensus
	baxosInstance := baxos.replicatedLog[instance]

	if baxosInstance.proposerBookkeeping.highestSeenAcceptedValue == nil && len(rp.incomingWriteRequests) > 0 {
		baxosInstance.proposerBookkeeping.highestSeenAcceptedValue = rp.incomingWriteRequests[0]
	}

	// propose message
	baxosInstance.proposerBookkeeping.proposedValue = baxosInstance.proposerBookkeeping.highestSeenAcceptedValue

	for _, replicaId := range rp.replicaNodes {
		proposeRequest := common.ProposeRequest {
			InstanceNumber: int64(instance),
			ProposeBallot:  baxosInstance.proposerBookkeeping.preparedBallot,
			ProposeValue:   baxosInstance.proposerBookkeeping.proposedValue,
			Sender:         int64(rp.id),
		}
		rp.outgoingChan <- common.Message{
			From:    rp.id,
			To:      replicaId,
			RpcPair: &common.RPCPair{Code: rp.messageCodes.ProposeRequest, Obj: &proposeRequest},
		}
	}
	rp.debug(fmt.Sprintf("PROPOSER: Instance %d, Ballot (%d, %d): Broadcast propose", instance,
		baxosInstance.proposerBookkeeping.preparedBallot.ReplicaId,
		baxosInstance.proposerBookkeeping.preparedBallot.Number), 2)
}

/*
	handler for accept messages
*/

func (rp *Replica) handleAccept(message *common.AcceptReply) {
	baxos := rp.baxosConsensus
	if baxos.isBackingOff {
		rp.debug("PROPOSER: Already backing off, hence ignoring the promise", 3)
		return
	}
	baxosInstance := baxos.replicatedLog[message.InstanceNumber]
	if baxosInstance.decided {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already decided, hence ignoring the accept", message.InstanceNumber), 3)
		return
	}

	if baxosInstance.proposerBookkeeping.numSuccessfulAccepts >= baxos.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Already enough accepts, hence ignoring the accept", message.InstanceNumber), 3)
		return
	}

	if !message.Accept {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Accept rejected, hence ignoring the accept", message.InstanceNumber), 3)
		return
	}

	rp.debug(fmt.Sprintf("PROPOSER: Instance %d, Ballot (%d, %d): Received an accept, hence incrementing the accept count",
		message.InstanceNumber,
		message.AcceptBallot.ReplicaId,
		message.AcceptBallot.Number), 3)
	
	baxosInstance.proposerBookkeeping.numSuccessfulAccepts++
	

	if baxosInstance.proposerBookkeeping.numSuccessfulAccepts == rp.baxosConsensus.quorumSize {
		rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Received quorum of accepts, hence deciding", message.InstanceNumber), 3)
		
		baxosInstance.decided = true
		baxosInstance.decidedValue = baxosInstance.proposerBookkeeping.proposedValue

		rp.baxosConsensus.isPreparing = false
		rp.baxosConsensus.isBackingOff = false

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

		for _, replicaId := range rp.replicaNodes {
			decideInfo := common.DecideInfo {
				InstanceNumber: int64(message.InstanceNumber),
				DecidedValue:   baxosInstance.decidedValue,
			}
			rp.outgoingChan <- common.Message {
				From:    rp.id,
				To:      replicaId,
				RpcPair: &common.RPCPair{Code: rp.messageCodes.DecideInfo, Obj: &decideInfo},
			}
			rp.debug(fmt.Sprintf("PROPOSER: Instance %d: Sent decide info to %d", message.InstanceNumber, replicaId), 3)
		}
	}
}