package src

import (
	"baxos/common"
	"fmt"
)

/*
	Send a print log request to all the replicas
*/

func (cl *Client) SendPrintLogRequest() {
	cl.debug("Sending print log request to all replicas", 0)
	
	for replicaId := range cl.replicaNodes {
		printLogRequest := common.PrintLog{}
		rpcPair := common.RPCPair {
			Code: cl.messageCodes.PrintLog,
			Obj:  &printLogRequest,
		}
		cl.outgoingChan <- common.Message {
			From:    cl.id,
			To:      replicaId,
			RpcPair: &rpcPair,
		}
		cl.debug(fmt.Sprintf("Sent print log request to %d", replicaId), 0)
	}
}
