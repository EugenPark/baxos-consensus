package src

import (
	"baxos/common"
	"fmt"
)

func (rp *Replica) runBaxosReader() {
	messageTmpl := "Reader: Received a message of type %s"
	for baxosMessage := range rp.baxosConsensus.readerChan {
		switch baxosMessage.code {
		case rp.messageCodes.ReadRequest:
			readRequest := baxosMessage.message.(*common.ReadRequest)
			msgType := "read"
			msg := fmt.Sprintf(messageTmpl, msgType)
			rp.debug(msg, 0)
			rp.handleReadRequest(readRequest)
		
		default:
			panic(fmt.Sprintf("Unknown message type for reader %d", baxosMessage.code))
		}
	}
}

func (rp *Replica) handleReadRequest(request *common.ReadRequest) {

	baxos := rp.baxosConsensus

	decidedCommand := &common.Command{
		Value: "",
	}

	if baxos.lastCommittedLogIndex > 0 {
		decidedCommand = baxos.replicatedLog[baxos.lastCommittedLogIndex].decidedValue.Command
	}

	rp.outgoingChan <- common.Message{
		From: rp.id,
		To:   int(request.Sender),
		RpcPair: &common.RPCPair{
			Code: rp.messageCodes.ReadResponse,
			Obj:  &common.ReadResponse{
				UniqueId: request.UniqueId,
				Sender:  int64(rp.id),
				InstanceNumber: int64(baxos.lastCommittedLogIndex),
				Command: decidedCommand,
			},
		},
	}
}