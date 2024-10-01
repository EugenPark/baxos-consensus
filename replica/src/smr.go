package src

import (
	"baxos/common"
	"fmt"
)

// add the client batch to buffer and propose

func (rp *Replica) handleWriteRequest(request *common.WriteRequest) {
	rp.incomingWriteRequests = append(rp.incomingWriteRequests, request)
	rp.tryPrepare()
}

func (rp *Replica) handleReadRequest(request *common.ReadRequest) {
	rp.incomingReadRequests[request.UniqueId] = &ReadRequestInstance{
		responses: make([]*common.ReadPromise, 0),
	}
	rp.sendReadPrepare(request)
}

func (rp *Replica) sendClientReadResponse(uniqueId string) {
	var latestIndex int64
	latestIndex = -1
	latestCommand := &common.Command{}
	var to int

	for _, response := range rp.incomingReadRequests[uniqueId].responses {
		if response.Index > latestIndex {
			latestIndex = response.Index
			latestCommand = response.Command
			to = int(response.ReadRequest.Sender)
		}
	}

	rp.outgoingChan <- common.Message{
		From: rp.id,
		To:   to,
		RpcPair: &common.RPCPair{
			Code: rp.messageCodes.ReadResponse,
			Obj:  &common.ReadResponse{
				UniqueId: uniqueId,
				Command:  latestCommand,
			},
		},
	}
}

// send back the client responses
func (rp *Replica) sendClientResponse(response *common.WriteResponse, to int) {
	if to == -1 {
		panic("Invalid client id")
	}
	rp.outgoingChan <- common.Message {
		From: rp.id,
		To:   to,
		RpcPair: &common.RPCPair {
			Code: rp.messageCodes.WriteResponse,
			Obj:  response,
		},
	}
	rp.debug(fmt.Sprintf("sent client response to %d", response.Sender), 0)
}
