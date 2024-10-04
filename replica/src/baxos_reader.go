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

		case rp.messageCodes.RinseRequest:
			rinseRequest := baxosMessage.message.(*common.RinseRequest)
			msgType := "rinse"
			msg := fmt.Sprintf(messageTmpl, msgType)
			rp.debug(msg, 0)
			rp.handleRinseRequest(rinseRequest)
		
		default:
			panic(fmt.Sprintf("Unknown message type for reader %d", baxosMessage.code))
		}
	}
}

func (rp *Replica) handleReadRequest(request *common.ReadRequest) {

	baxos := rp.baxosConsensus

	highestAcceptedIndex := 0

	// Find the latest accepted ballot slot
	for i := len(baxos.replicatedLog) - 1 ; i > 0; i-- {
		acceptedBallot := baxos.replicatedLog[i].acceptorBookkeeping.acceptedBallot
		if acceptedBallot.IsGreaterThan(&common.Ballot{ReplicaId: -1, Number: -1}) {
			highestAcceptedIndex = i
			break
		}
	}

	rp.debug(fmt.Sprintf("Latest index for read request with id %s is %d", request.UniqueId, highestAcceptedIndex), 0)

	rp.outgoingChan <- common.Message{
		From: rp.id,
		To:   int(request.Sender),
		RpcPair: &common.RPCPair{
			Code: rp.messageCodes.ReadResponse,
			Obj:  &common.ReadResponse{
				UniqueId: request.UniqueId,
				Sender:  int64(rp.id),
				InstanceNumber: int64(highestAcceptedIndex),
			},
		},
	}
}

func (rp *Replica) handleRinseRequest(request *common.RinseRequest) {
	baxos := rp.baxosConsensus

	var isDecided bool

	if int(request.InstanceNumber) >= len(baxos.replicatedLog) {
		isDecided = false
	}

	isDecided = baxos.replicatedLog[request.InstanceNumber].decided

	var command *common.Command

	// skip genesis slot 0
	if isDecided && request.InstanceNumber > 0 {
		command = baxos.replicatedLog[request.InstanceNumber].decidedValue.Command
	}

	rp.debug(fmt.Sprintf("Rinse request for instance %d and id %s decided: %t", request.InstanceNumber,request.UniqueId, isDecided), 0)

	rp.outgoingChan <- common.Message{
		From: rp.id,
		To:   int(request.Sender),
		RpcPair: &common.RPCPair{
			Code: rp.messageCodes.RinseResponse,
			Obj:  &common.RinseResponse{
				UniqueId:      request.UniqueId,
				Sender:        int64(rp.id),
				InstanceNumber: request.InstanceNumber,
				Decided:       isDecided,
				Command: command,
			},
		},
	}
}