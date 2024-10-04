package src

import (
	"baxos/common"
	"fmt"
)

/*
	Logic for prepare message, check if it is possible to promise for the specified instance
*/

func (rp *Replica) processPrepare(message *common.PrepareRequest) (*common.PromiseReply, *common.DecideInfo) {
	if message == nil {
		panic("nil prepare message")
	}

	rp.createInstance(int(message.InstanceNumber))

	baxos := rp.baxosConsensus
	baxosInstance := baxos.replicatedLog[message.InstanceNumber]

	if baxosInstance.decided {
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Already decided, hence sending a decide info", message.InstanceNumber), 1)
		return nil, &common.DecideInfo {
			InstanceNumber: message.InstanceNumber,
			DecidedValue:   baxosInstance.decidedValue,
		}
	}

	if baxosInstance.acceptorBookkeeping.promisedBallot.IsGreaterThan(message.PrepareBallot) {
		return nil, nil
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
	}, nil
}

/*
	Handler for prepare message
*/

func (rp *Replica) handlePrepare(message *common.PrepareRequest) {
	promiseReply, decideInfo := rp.processPrepare(message)

	if promiseReply != nil && decideInfo != nil {
		panic("Both accept reply and decide info are not nil")
	}

	if promiseReply == nil && decideInfo == nil {
		return
	}

	if promiseReply != nil {
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

	if decideInfo != nil {
		rp.outgoingChan <- common.Message {
			From:    rp.id,
			To:      int(message.Sender),
			RpcPair: &common.RPCPair{
				Code: rp.messageCodes.DecideInfo,
				Obj:  decideInfo,
			},
		}
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Sent decide info to %d", message.InstanceNumber, message.Sender), 0)
	}
}

// logic for propose message

func (rp *Replica) processPropose(message *common.ProposeRequest) (*common.AcceptReply, *common.DecideInfo) {
	rp.createInstance(int(message.InstanceNumber))
	baxos := rp.baxosConsensus
	baxosInstance := baxos.replicatedLog[message.InstanceNumber]

	if baxosInstance.decided {
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Already decided, hence sending a accept reply with the decided value", message.InstanceNumber), 3)
		return nil, &common.DecideInfo {
			InstanceNumber: message.InstanceNumber,
			DecidedValue:   baxosInstance.decidedValue,
		}
	}

	if baxosInstance.acceptorBookkeeping.promisedBallot.IsGreaterThan(message.ProposeBallot) {
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d, Ballot (%d, %d): Propose rejected", message.InstanceNumber, message.ProposeBallot.ReplicaId, message.ProposeBallot.Number), 3)
		return nil, nil
	}
	
	baxosInstance.acceptorBookkeeping.acceptedBallot = message.ProposeBallot
	baxosInstance.acceptorBookkeeping.acceptedValue = message.ProposeValue
	rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d, Ballot (%d, %d): Accepted propose, hence sending a accept", message.InstanceNumber, message.ProposeBallot.ReplicaId, message.ProposeBallot.Number), 3)
	
	return &common.AcceptReply{
		InstanceNumber: message.InstanceNumber,
		Accept:         true,
		AcceptBallot:   message.ProposeBallot,
		Sender:         message.Sender,
	}, nil
}

/*
	handler for propose message
*/

func (rp *Replica) handlePropose(message *common.ProposeRequest) {
	// handle the propose slot
	acceptReply, decideInfo := rp.processPropose(message)

	if acceptReply != nil && decideInfo != nil {
		panic("Both accept reply and decide info are not nil")
	}

	if acceptReply == nil && decideInfo == nil {
		return
	}

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

	if decideInfo != nil {
		rp.outgoingChan <- common.Message {
			From:    rp.id,
			To:      int(message.Sender),
			RpcPair: &common.RPCPair{
				Code: rp.messageCodes.DecideInfo,
				Obj:  decideInfo,
			},
		}
		rp.debug(fmt.Sprintf("ACCEPTOR: Instance %d: Sent decide info to %d", message.InstanceNumber, message.Sender), 0)
	}
}
