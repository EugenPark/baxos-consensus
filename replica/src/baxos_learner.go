package src

import (
	"baxos/common"
	"fmt"
)

func (rp *Replica) handleDecideInfo(message *common.DecideInfo) {
	rp.debug(fmt.Sprintf("LEARNER: Instance %d: Received a decided value", message.InstanceNumber), 1)
	rp.createInstance(int(message.InstanceNumber))
	baxosInstance := rp.baxosConsensus.replicatedLog[message.InstanceNumber]
	baxosInstance.decided = true
	baxosInstance.decidedValue = message.DecidedValue
	rp.updateSMR()
	rp.debug(fmt.Sprintf("LEARNER: Instance %d: Decided value: %s", message.InstanceNumber, message.DecidedValue.Command.Value), 3)
}