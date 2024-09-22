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

func (cl *Client) handleClientResponseBatch(batch *common.ClientBatch) {
	if cl.finished {
		return
	}

	// check if key already exists
	_, ok := cl.receivedResponses[batch.UniqueId]
	if ok {
		return
	}

	cl.receivedResponses[batch.UniqueId] = &RequestBatch {
		batch: batch,
		time:  time.Now(), // record the time when the response was received
	}
	cl.debug("Added response Batch with id " + batch.UniqueId, 0)
	
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
	cl.finished = true
	fmt.Printf("Finish sending requests \n")
	cl.computeStats()
}

func (cl *Client) generateRequests() {
	batchCounter := 0
	for !cl.finished {            
		numRequests := 0
		var requests []*common.SingleOperation
		// this loop collects requests until the minimum batch size is met OR the batch time is timeout
		for numRequests < cl.clientBatchSize {
			<-cl.arrivalChan // keep collecting new requests arrivals
			cl.debug("New request arrival", 0)
			requests = append(requests, &common.SingleOperation {
				Command: fmt.Sprintf("%d%v%v", rand.Intn(2),
					cl.RandString(cl.keyLen),
					cl.RandString(cl.valueLen)),
			})
			numRequests++
		}
		
		uniqueId := fmt.Sprintf("%d.%d", cl.id, batchCounter)
		// create a new client batch
		batch := common.ClientBatch {
			UniqueId: uniqueId, // this is a unique string id,
			Requests: requests,
			Sender:   int64(cl.id),
		}
		
		rpcPair := common.RPCPair {
			Code: cl.messageCodes.ClientBatchRpc,
			Obj:  &batch,
		}

		for _, replicaNode := range cl.replicaNodes {
			cl.outgoingChan <- common.Message {
				From:  cl.id,
				To: replicaNode.id,
				RpcPair:  &rpcPair,
			}

			cl.debug(fmt.Sprintf("Sent batch with id %s and len %d to replica with id %d", uniqueId, len(requests), replicaNode.id), 0)
		}

		batchCounter++

		cl.sentRequests[uniqueId] = &RequestBatch {
			batch: &batch,
			time:  time.Now(),
		}
	}
}

/*
	After the request arrival time is arrived, inform the request generators
*/

func (cl *Client) startScheduler() {
	cl.startTime = time.Now()
	for !cl.finished {
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
