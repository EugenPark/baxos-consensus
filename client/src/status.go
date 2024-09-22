package src

import (
	"baxos/common"
	"fmt"
	"strconv"
	"time"
)

/*
	when a status response is received, print it to console
*/

func (cl *Client) handleClientStatusResponse(response *common.Status) {
	fmt.Printf("status response from %v for type %v\n", response.Sender, response.Type)
}

/*
	Send a status request to all the replicas
*/

func (cl *Client) SendStatus(operationType int) {
	cl.debug("Sending status request to all replicas", 0)
	
	for name := range cl.replicaNodes {

		statusRequest := common.Status {
			Type:   int32(operationType),
			Note:   "",
			Sender: int64(cl.id),
		}

		rpcPair := common.RPCPair {
			Code: cl.messageCodes.StatusRPC,
			Obj:  &statusRequest,
		}
		cl.outgoingChan <- common.Message {
			From:    cl.id,
			To:      cl.replicaNodes[name].id,
			RpcPair: &rpcPair,
		}
		cl.debug("Sent status to "+strconv.Itoa(int(name)), 0)
	}
	time.Sleep(time.Duration(statusTimeout) * time.Second)
}
