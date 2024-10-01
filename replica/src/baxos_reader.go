package src

import (
	"baxos/common"
	"fmt"
)

// Reads

func (rp *Replica) sendReadPrepare(request *common.ReadRequest) {
	for _, replicaId := range rp.replicaNodes {
		rp.outgoingChan <- common.Message {
			From:    rp.id,
			To:      replicaId,
			RpcPair: &common.RPCPair{Code: rp.messageCodes.ReadPrepare, Obj: &common.ReadPrepare{
				UniqueId: request.UniqueId,
				Sender:  int64(rp.id),
				ReadRequest: request,
			}},
		}
	}
}


func (rp *Replica) handleReadPrepare(prepare *common.ReadPrepare) {
	baxos := rp.baxosConsensus
	lastDecidedCommand := &common.Command{}

	if baxos.lastCommittedLogIndex > 0 && baxos.replicatedLog[baxos.lastCommittedLogIndex].decided {
		lastDecidedCommand = baxos.replicatedLog[rp.baxosConsensus.lastCommittedLogIndex].decidedValue.Command
	}
	
	rp.outgoingChan <- common.Message {
		From: rp.id,
		To:   int(prepare.Sender),
		RpcPair: &common.RPCPair {
			Code: rp.messageCodes.ReadPromise,
			Obj:  &common.ReadPromise {
				UniqueId: prepare.UniqueId,
				Command: lastDecidedCommand,
				Index:  int64(baxos.lastCommittedLogIndex),
				Sender: int64(rp.id),
				ReadRequest: prepare.ReadRequest,
			},
		},
	}

	rp.debug(fmt.Sprintf("ACCEPTOR: Sent read promise to %d for id %s", prepare.Sender, prepare.UniqueId), 0)
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
		rp.sendClientReadResponse(uniqueId)
	}
}