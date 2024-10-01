package src

import (
	"baxos/common"
	"fmt"
)

/*
	Logic for prepare message, check if it is possible to promise for the specified instance
*/

func (rp *Replica) processPrepare(message *common.PrepareRequest) *common.PromiseReply {
	if message == nil {
		panic("nil prepare message")
	}

	rp.createInstance(int(message.InstanceNumber))

	baxos := rp.baxosConsensus
	baxosInstance := &baxos.replicatedLog[message.InstanceNumber]

	if baxosInstance.decided {
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Already decided, hence sending a promise reply with the decided value", message.InstanceNumber), 1)
		return &common.PromiseReply {
			InstanceNumber: message.InstanceNumber,
			Promise:        false,
			Decided:        true,
			DecidedValue:   baxosInstance.decidedValue,
			Sender:         int64(rp.id),
		}
	}

	if baxosInstance.acceptorBookkeeping.promisedBallot.IsGreaterThan(message.PrepareBallot) {
		return nil
	}
	
	baxosInstance.acceptorBookkeeping.promisedBallot = message.PrepareBallot
	rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d, Ballot (%d, %d): Prepare accepted, hence sending a promise reply", message.InstanceNumber, message.PrepareBallot.ReplicaId, message.PrepareBallot.Number), 3)
		

	return &common.PromiseReply {
		InstanceNumber:     message.InstanceNumber,
		Promise:            true,
		LastPromisedBallot: message.PrepareBallot,
		LastAcceptedBallot: baxosInstance.acceptorBookkeeping.acceptedBallot,
		LastAcceptedValue:  baxosInstance.acceptorBookkeeping.acceptedValue,
		Sender:             int64(rp.id),
	}	
}

/*
	Handler for prepare message
*/

func (rp *Replica) handlePrepare(message *common.PrepareRequest) {
	promiseReply := rp.processPrepare(message)

	if promiseReply == nil {
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d, Ballot (%d, %d): Prepare ignored", message.InstanceNumber, message.PrepareBallot.ReplicaId, message.PrepareBallot.Number), 1)
		return
	}

	rpcPair := common.RPCPair{
		Code: rp.messageCodes.PromiseReply,
		Obj:  promiseReply,
	}

	rp.outgoingChan <- common.Message {
		From:    rp.id,
		To:      int(message.Sender),
		RpcPair: &rpcPair,
	}
	rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Sent a promise response to %d", message.InstanceNumber, message.Sender), 2)
}

// logic for propose message

func (rp *Replica) processPropose(message *common.ProposeRequest) *common.AcceptReply {
	rp.createInstance(int(message.InstanceNumber))
	baxos := rp.baxosConsensus
	baxosInstance := &baxos.replicatedLog[message.InstanceNumber]

	if baxosInstance.decided {
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Already decided, hence sending a accept reply with the decided value", message.InstanceNumber), 4)
		
		return &common.AcceptReply{
			InstanceNumber: message.InstanceNumber,
			Accept:         false,
			Decided:        true,
			DecidedValue:   baxosInstance.decidedValue,
			Sender:         int64(rp.id),
		}
	}

	promisedBallot := rp.baxosConsensus.replicatedLog[message.InstanceNumber].acceptorBookkeeping.promisedBallot
	if !promisedBallot.IsGreaterThan(message.ProposeBallot) {
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].acceptorBookkeeping.acceptedBallot = message.ProposeBallot
		rp.baxosConsensus.replicatedLog[message.InstanceNumber].acceptorBookkeeping.acceptedValue = message.ProposeValue
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d, Ballot (%d, %d): Accepted propose, hence sending a accept", message.InstanceNumber, message.ProposeBallot.ReplicaId, message.ProposeBallot.Number), 3)
		
		return &common.AcceptReply{
			InstanceNumber: message.InstanceNumber,
			Accept:         true,
			AcceptBallot:   message.ProposeBallot,
			Sender:         message.Sender,
		}
	}

	rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d, Ballot (%d, %d): Propose rejected", message.InstanceNumber, message.ProposeBallot.ReplicaId, message.ProposeBallot.Number), 3)
	return nil
}

/*
	handler for propose message
*/

func (rp *Replica) handlePropose(message *common.ProposeRequest) {

	// handle the propose slot
	acceptReply := rp.processPropose(message)

	if acceptReply != nil {
		rp.outgoingChan <- common.Message {
			From:    rp.id,
			To:      int(message.Sender),
			RpcPair: &common.RPCPair{
				Code: rp.messageCodes.AcceptReply,
				Obj:  acceptReply,
			},
		}
		
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Sent accept message to %d", message.InstanceNumber, message.Sender), 0)
		
	}

	// handle the decided slot
	if message.DecideInfo != nil {
		rp.createInstance(int(message.DecideInfo.InstanceNumber))
		if !rp.baxosConsensus.replicatedLog[message.DecideInfo.InstanceNumber].decided {
			rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Decided using decided value in propose", message.DecideInfo.InstanceNumber), 2)
			
			rp.baxosConsensus.replicatedLog[message.DecideInfo.InstanceNumber].decided = true
			rp.baxosConsensus.replicatedLog[message.DecideInfo.InstanceNumber].decidedValue = message.DecideInfo.DecidedValue
			rp.updateSMR()
		}
	}
}

func (rp *Replica) handleReadPrepare(prepare *common.ReadPrepare, from int) {
	baxos := rp.baxosConsensus
	lastDecidedCommand := &common.Command{}

	if baxos.lastCommittedLogIndex > 0 && baxos.replicatedLog[baxos.lastCommittedLogIndex].decided {
		lastDecidedCommand = baxos.replicatedLog[rp.baxosConsensus.lastCommittedLogIndex].decidedValue.Command
	}
	
	rp.outgoingChan <- common.Message {
		From: rp.id,
		To:   from,
		RpcPair: &common.RPCPair {
			Code: rp.messageCodes.ReadPromise,
			Obj:  &common.ReadPromise {
				UniqueId: prepare.UniqueId,
				Command: lastDecidedCommand,
				Index:  int64(baxos.lastCommittedLogIndex),
				Sender: prepare.Sender,
			},
		},
	}

	rp.debug(fmt.Sprintf("ACCEPTOR: Sent read promise to %d for id %s", prepare.Sender, prepare.UniqueId), 0)
}
