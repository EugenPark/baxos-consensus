package main

import (
	"baxos/client/src"
	"baxos/common"
	"flag"
	"fmt"
	"time"
)

func main() {
	id := flag.Int("id", 51, "name of the client as specified in the local-configuration.yml")
	configFile := flag.String("config", "configuration/local-configuration.yml", "configuration file")
	logFilePath := flag.String("logFilePath", "logs/", "log file path")

	flag.Parse()

	cfg, err := common.ParseConfig(*configFile, *id)
	if err != nil {
		panic(err.Error())
	}

	rpcConfigs := []common.RPCConfig{
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
			MsgObj: new(common.DecideAck),
			Code:   common.GetRPCCodes().DecideAck,
		},
	}

	outgoingChan := make(chan common.Message, 10000000)
	incomingChan := make(chan common.Message, 10000000)

	network := common.NewNetwork(int(*id), cfg, outgoingChan, incomingChan)
	network.Init(rpcConfigs, cfg)

	cl := src.New(int(*id), *logFilePath, cfg, incomingChan, outgoingChan)
	cl.Init(cfg)

	go network.Run()
	go cl.Run()

	time.Sleep(time.Duration(5) * time.Second)

	cl.SendRequests()
	cl.Finished = true
	fmt.Printf("Finish sending requests \n")
	cl.SendPrintLogRequest()
	cl.ComputeStats()
	time.Sleep(time.Duration(cfg.Flags.NetworkFlags.ArtificialLatency) + time.Duration(5) * time.Second) // wait to ensure all logs are printed
}
