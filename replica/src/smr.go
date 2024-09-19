package src

import (
	"baxos/common"
	"strconv"
)

// add the client batch to buffer and propose

func (rp *Replica) handleClientBatch(batch *common.ClientBatch) {
	rp.incomingRequests = append(rp.incomingRequests, batch)
	rp.tryPropose()
}

// call the state machine

func (rp *Replica) updateApplicationLogic(requests []*common.ClientBatch) []*common.ClientBatch {
	return rp.state.Execute(requests)
}

// send back the client responses

func (rp *Replica) sendClientResponses(responses []*common.ClientBatch) {
	for i := 0; i < len(responses); i++ {
		rp.outgoingChan <- common.Message {
			From: rp.id,
			To:   int32(responses[i].Sender),
			RpcPair: &common.RPCPair {
				Code: rp.messageCodes.ClientBatchRpc,
				Obj:  responses[i],
			},
		}
		
		rp.debug("sent client response to "+strconv.Itoa(int(responses[i].Sender)), 0)
		
	}
}
