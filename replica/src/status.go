package src

func (rp *Replica) handlePrintLog() {
	if !rp.logPrinted {
		rp.logPrinted = true
		rp.printBaxosLogConsensus() // this is for consensus testing purposes
	}
}
