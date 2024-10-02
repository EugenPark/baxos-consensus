package main

import (
	"baxos/common"
	"baxos/replica/src"
	"flag"
	"fmt"
	"os"
	"sync"
)

func main() {
	id := flag.Int("id", 1, "Id of the replica as specified in the local-configuration.yml")
	configFile := flag.String("config", "configuration/local-configuration.yml", "configuration file")
	logFilePath := flag.String("logFilePath", "logs/", "log file path")

	flag.Parse()

	cfg, err := common.ParseConfig(*configFile, *id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		panic(err)
	}

	rpcConfigs := []common.RPCConfig {
		{
			MsgObj: new(common.WriteRequest),
			Code:   common.GetRPCCodes().WriteRequest,
		},
		{
			MsgObj: new(common.WriteResponse),
			Code:   common.GetRPCCodes().WriteResponse,
		},
		{
			MsgObj: new(common.ReadRequest),
			Code:   common.GetRPCCodes().ReadRequest,
		},
		{
			MsgObj: new(common.ReadResponse),
			Code:   common.GetRPCCodes().ReadResponse,
		},
		{
			MsgObj: new(common.PrintLog),
			Code:   common.GetRPCCodes().PrintLog,
		},
		{
			MsgObj: new(common.PrepareRequest),
			Code: common.GetRPCCodes().PrepareRequest,
		},
		{
			MsgObj: new(common.PromiseReply), 
			Code: common.GetRPCCodes().PromiseReply,
		},
		{
			MsgObj: new(common.ProposeRequest),
			Code: common.GetRPCCodes().ProposeRequest,
		},
		{
			MsgObj: new(common.AcceptReply), 
			Code: common.GetRPCCodes().AcceptReply,
		},
		{
			Code: common.GetRPCCodes().DecideInfo,
			MsgObj: new(common.DecideInfo),
		},
	}

	outgoingChan := make(chan common.Message, 1000000)
	incomingChan := make(chan common.Message, 1000000)

	network := common.NewNetwork(*id, cfg, outgoingChan, incomingChan)
	network.Init(rpcConfigs, cfg)

	rp := src.New(int(*id), *logFilePath, cfg, incomingChan, outgoingChan)
	rp.Init(cfg)

	var wg sync.WaitGroup
	wg.Add(1)

	go network.Run()
	go func() {
		rp.Run()
		wg.Done()
	}()

	wg.Wait()
}
