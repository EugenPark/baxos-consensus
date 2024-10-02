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
