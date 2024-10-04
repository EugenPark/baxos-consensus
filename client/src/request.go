package src

import (
	"baxos/common"
	"fmt"
	"math/rand"
	"time"
	"unsafe"
)

/*
	Upon receiving a client response, add the request to the received requests map
*/

func (cl *Client) handleWriteResponse(response *common.WriteResponse) {
	if cl.Finished {
		return
	}

	id := response.UniqueId

	// check if key already exists
	cl.requestsMutex.Lock()
	defer cl.requestsMutex.Unlock()
	writeRequest, ok := cl.requests[id]
	if !ok {
		cl.debug(fmt.Sprintf("Received response for a write request with id %s that was not sent", id), 0)
		panic("should not happen")
	}
	if writeRequest.isCompleted() {
		cl.debug(fmt.Sprintf("Already received write response for this id %s", id), 0)
		return
	}

	writeRequest.command = response.Command.Value

	writeRequest.endTime()
	cl.debug("Added write response with id " + id, 1)
}

func (cl *Client) handleReadResponse(response *common.ReadResponse) {
	if cl.Finished {
		return
	}
	
	if len(cl.readResponses[response.UniqueId]) >= cl.quorumSize {
		return
	}

	cl.readResponses[response.UniqueId] = append(cl.readResponses[response.UniqueId], response)
	cl.debug(fmt.Sprintf("Received %d responses for read request with id %s", len(cl.readResponses[response.UniqueId]), response.UniqueId), 0)

	if len(cl.readResponses[response.UniqueId]) == cl.quorumSize {
		cl.debug(fmt.Sprintf("Received quorum for read request with id %s", response.UniqueId), 0)
		latestDecidedResponse := cl.readResponses[response.UniqueId][0]

		for _, readResponse := range cl.readResponses[response.UniqueId] {
			if readResponse.InstanceNumber > latestDecidedResponse.InstanceNumber {
				latestDecidedResponse = readResponse
			}
		}

		cl.debug(fmt.Sprintf("Latest decided instance number for read request with id %s is %d", latestDecidedResponse.UniqueId, latestDecidedResponse.InstanceNumber), 0)

		cl.requestsMutex.Lock()
		defer cl.requestsMutex.Unlock()
		// check if key already exists
		id := latestDecidedResponse.UniqueId
		readRequest, ok := cl.requests[id]
		if !ok {
			cl.debug(fmt.Sprintf("Received response for a read request with id %s that was not sent", id), 0)
			panic("should not happen")
		}

		if readRequest.isCompleted() {
			cl.debug(fmt.Sprintf("Already received read response for this id %s", id), 0)
			return
		}

		readRequest.endTime()
		if latestDecidedResponse.Command == nil {
			readRequest.command = ""
			cl.debug("Empty read received", 0)
		} else {
			readRequest.command = latestDecidedResponse.Command.Value
			cl.debug(fmt.Sprintf("Added read response with id %s and value %s", id, latestDecidedResponse.Command.Value), 0)
		}
	}
}

/*
	start the poisson arrival process (put arrivals to arrivalTimeChan) in a separate thread
	start request generation processes  (get arrivals from arrivalTimeChan and generate batches and send them) in separate threads, and send them to all replicas, and write batch to the correct array in sentRequests
	start the scheduler that schedules new requests
	the thread sleeps for test duration and then starts processing the responses. This is to handle inflight responses after the test duration
*/

func (cl *Client) SendRequests() {
	go cl.generateRequests()
	go cl.startScheduler()
	time.Sleep(time.Duration(cl.testDuration) * time.Second)
}

func (cl *Client) generateWriteRPCPair(uniqueId string) common.RPCPair {

	command := fmt.Sprintf("%d%v%v", rand.Intn(2), cl.RandString(cl.keyLen), cl.RandString(cl.valueLen))
	// create a new client batch
	writeRequest := common.WriteRequest {
		UniqueId: uniqueId, // this is a unique string id,
		Command: &common.Command {
			Value: command,
		},
		Sender:   int64(cl.id),
	}

	request := ClientRequest {
		id: uniqueId,
		command: command,
		requestType: "write",
	}
	request.startTime()

	cl.requestsMutex.Lock()
	defer cl.requestsMutex.Unlock()
	cl.requests[uniqueId] = &request
	
	return common.RPCPair {
		Code: cl.messageCodes.WriteRequest,
		Obj:  &writeRequest,
	}
}

func (cl *Client) generateReadRPCPair(uniqueId string) common.RPCPair {
	request := ClientRequest {
		id: uniqueId,
		requestType: "read",
	}
	request.startTime()

	cl.requestsMutex.Lock()
	defer cl.requestsMutex.Unlock()
	cl.requests[uniqueId] = &request

	return common.RPCPair {
		Code: cl.messageCodes.ReadRequest,
		Obj:  &common.ReadRequest {
			UniqueId: uniqueId,
			Sender:  int64(cl.id),
		},
	}
}

func (cl *Client) generateRequests() {
	requestCounter := 0
	for !cl.Finished {            		
		<-cl.arrivalChan
		cl.debug("New request arrival", 0)

		uniqueId := fmt.Sprintf("%d.%d", cl.id, requestCounter)
		
		var rpcPair common.RPCPair

		if rand.Float64() < cl.writeRequestRatio {
			rpcPair = cl.generateWriteRPCPair(uniqueId)
			cl.debug(fmt.Sprintf("Client %d: Generated a write request with id %s", cl.id, uniqueId), 0)
		} else {
			rpcPair = cl.generateReadRPCPair(uniqueId)
			cl.debug(fmt.Sprintf("Client %d: Generated a read request with id %s", cl.id, uniqueId), 0)
		}

		for _, replicaId := range cl.replicaNodes {
			cl.outgoingChan <- common.Message {
				From:  cl.id,
				To: replicaId,
				RpcPair:  &rpcPair,
			}

			cl.debug(fmt.Sprintf("Client %d: Sent a request with id %s to replica with id %d", cl.id, uniqueId, replicaId), 0)
		}
		requestCounter++
	}
}

/*
	After the request arrival time is arrived, inform the request generators
*/

func (cl *Client) startScheduler() {
	cl.startTime = time.Now()
	for !cl.Finished {
		// sleep for the next arrival time
		interArrivalTime := rand.ExpFloat64() / float64(cl.arrivalRate)
		time.Sleep(time.Duration(interArrivalTime * float64(time.Second)))
		cl.debug("New request generated", 0)
		cl.arrivalChan <- true
	}
}

/*
	random string generation adapted from the Rabia SOSP 2021 code base https://github.com/haochenpan/rabia/
*/

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // low conflict
	letterIdxBits = 6                                                      // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1                                   // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits                                     // # of letter indices fitting in 63 bits
)

/*
	generate a random string of length n
*/

func (cl *Client) RandString(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}
