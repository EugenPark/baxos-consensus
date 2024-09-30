package main

import (
	"baxos/common"
	"baxos/replica/src"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
)

func main() {
	id := flag.Int("id", 1, "Id of the replica as specified in the local-configuration.yml")
	region := flag.String("region", "", "region of the replica")
	configFile := flag.String("config", "configuration/local-configuration.yml", "configuration file")
	logFilePath := flag.String("logFilePath", "logs/", "log file path")
	debugOn := flag.Bool("debugOn", false, "false or true")
	debugLevel := flag.Int("debugLevel", -1, "debug level")
	roundTripTime := flag.Int("roundTripTime", 40, "round trip time in milliseconds")
	keyLen := flag.Int("keyLen", 8, "key length")
	valLen := flag.Int("valLen", 8, "value length")
	benchmarkMode := flag.Int("benchmarkMode", 0, "0: resident store, 1: redis")
	artificialLatencyFlag := flag.Bool("artificialLatency", false, "Should artificial latency be added")

	flag.Parse()

	var artificialLatency time.Duration

	if *artificialLatencyFlag {
		artificialLatency = time.Duration(*roundTripTime/2) * time.Millisecond
	} else {
		artificialLatency = 0
	}

	cfg, err := common.NewInstanceConfig(*configFile, *id)
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
			Code: common.GetRPCCodes().ReadPrepare,
			MsgObj: new(common.ReadPrepare),
		},
		{
			Code: common.GetRPCCodes().ReadPromise,
			MsgObj: new(common.ReadPromise),
		},
	}

	outgoingChan := make(chan common.Message, 1000000)
	incomingChan := make(chan common.Message, 1000000)

	network := common.NewNetwork(int(*id), (*debugLevel == 0 && *debugOn), artificialLatency, outgoingChan, incomingChan)
	network.Init(rpcConfigs, cfg)

	rp := src.New(int(*id), *logFilePath, *debugOn,
		          *debugLevel, *benchmarkMode, *keyLen, *valLen,
				  incomingChan, outgoingChan, *region)
	rp.Init(cfg, *roundTripTime)

	var wg sync.WaitGroup
	wg.Add(1)

	go network.Run()
	go func() {
		rp.Run()
		wg.Done()
	}()

	wg.Wait()
}
