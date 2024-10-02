package src

import "baxos/common"

func (rp *Replica) handleReadRequest(request *common.ReadRequest) {

	baxos := rp.baxosConsensus
	latestDecidedCommand := &common.Command{}

	if len(baxos.replicatedLog) > 0 && baxos.lastCommittedLogIndex > 0 {
		if !baxos.replicatedLog[baxos.lastCommittedLogIndex].decided {
			panic("should not happen")
		}
		latestDecidedCommand = baxos.replicatedLog[baxos.lastCommittedLogIndex].decidedValue.Command
	}

	rp.outgoingChan <- common.Message{
		From: rp.id,
		To:   int(request.Sender),
		RpcPair: &common.RPCPair{
			Code: rp.messageCodes.ReadResponse,
			Obj:  &common.ReadResponse{
				UniqueId: request.UniqueId,
				Command:  latestDecidedCommand,
				Sender:  int64(rp.id),
				InstanceNumber: int64(baxos.lastCommittedLogIndex),
			},
		},
	}
}