package src

import (
	"baxos/common"
	"fmt"
)

func (rp *Replica) runBaxosLearner() {
	messageTmpl := "Instance %d: Received a message of type %s"
	for baxosMessage := range rp.baxosConsensus.learnerChan {
		switch baxosMessage.code {
		case rp.messageCodes.DecideInfo:
			decideInfo := baxosMessage.message.(*common.DecideInfo)
			msgType := "decide"
			msg := fmt.Sprintf(messageTmpl, decideInfo.InstanceNumber, msgType)
			rp.debug(msg, 3)
			rp.handleDecideInfo(decideInfo)

		default:
			panic(fmt.Sprintf("Unknown message type in Learner %d", baxosMessage.code))
		}
	}
}

func (rp *Replica) handleDecideInfo(message *common.DecideInfo) {
	rp.debug(fmt.Sprintf("LEARNER: Instance %d: Received a decided value", message.InstanceNumber), 1)
	rp.createInstance(int(message.InstanceNumber))
	baxosInstance := rp.baxosConsensus.replicatedLog[message.InstanceNumber]
	baxosInstance.decided = true
	baxosInstance.decidedValue = message.DecidedValue
	rp.updateSMR()
	decidedAck := &common.DecideAck {
		InstanceNumber: message.InstanceNumber,
		Sender: 	   int64(rp.id),
	}
	rp.outgoingChan <- common.Message {
		From:    rp.id,
		To:      int(message.Sender),
		RpcPair: &common.RPCPair{Code: rp.messageCodes.DecideAck, Obj: decidedAck},
	}
	if message.DecidedValue.Command != nil {
		rp.debug(fmt.Sprintf("LEARNER: Instance %d: Decided value: %s", message.InstanceNumber, message.DecidedValue.Command.Value), 0)
	} else {
		rp.debug(fmt.Sprintf("LEARNER: Instance %d: Decided value: <empty>", message.InstanceNumber), 0)
	}
}