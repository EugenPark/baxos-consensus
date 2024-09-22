package src

import (
	"baxos/common"
	"fmt"
)

// add the client batch to buffer and propose

func (rp *Replica) handleWriteRequest(request *common.WriteRequest) {
	rp.incomingRequests = append(rp.incomingRequests, request)
	rp.tryPropose()
}

// send back the client responses
func (rp *Replica) sendClientResponse(response *common.WriteResponse, to int32) {
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
