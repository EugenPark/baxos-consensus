package main

import (
	"baxos/client/src"
	"baxos/common"
	"flag"
	"time"
)

func main() {
	id := flag.Int64("id", 51, "name of the client as specified in the local-configuration.yml")
	region := flag.String("region", "", "region of the client")
	configFile := flag.String("config", "configuration/local-configuration.yml", "configuration file")
	logFilePath := flag.String("logFilePath", "logs/", "log file path")
	testDuration := flag.Int("testDuration", 60, "test duration in seconds")
	arrivalRate := flag.Float64("arrivalRate", 1000, "poisson arrival rate in requests per second")
	requestType := flag.String("requestType", "status", "request type: [status , request]")
	operationType := flag.Int("operationType", 1, "Type of operation for a status request: 1 (bootstrap server), 2: (print log)")
	debugOn := flag.Bool("debugOn", false, "false or true")
	debugLevel := flag.Int("debugLevel", -1, "debug level int")
	keyLen := flag.Int("keyLen", 8, "key length")
	valLen := flag.Int("valLen", 8, "value length")
	window := flag.Int64("window", 1000, "number of out standing client batches")
	artificialLatency := flag.Int("artificialLatency", 20000, "Duration of artificial latency when sending a message in micro seconds")
	artificialLatencyMultiplier := flag.Int("artificialLatencyMultiplier", 10, "By how much should the artificial latency be multiplied when sending to a different region")

	flag.Parse()

	cfg, err := common.NewInstanceConfig(*configFile, *id)
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
			MsgObj: new(common.Status),
			Code:   common.GetRPCCodes().StatusRPC,
		},
	}

	outgoingChan := make(chan common.Message, 10000000)
	incomingChan := make(chan common.Message, 10000000)

	network := common.NewNetwork(int32(*id), (*debugLevel == 0 && *debugOn), *artificialLatency, *artificialLatencyMultiplier, outgoingChan, incomingChan)
	network.Init(rpcConfigs, cfg)
	
	cl := src.New(int32(*id), *logFilePath, *testDuration, *arrivalRate, *requestType, *operationType, *debugOn, *debugLevel, *keyLen, *valLen, *window, incomingChan, outgoingChan, *region)
	cl.Init(cfg)

	go network.Run()
	go cl.Run()

	time.Sleep(time.Duration(5) * time.Second)

	if cl.RequestType == "status" {
		cl.SendStatus(cl.OperationType)
	} else if cl.RequestType == "request" {
		cl.SendRequests()
	}
}
