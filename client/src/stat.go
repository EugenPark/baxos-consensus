package src

import (
	"baxos/common"
	"fmt"
	"os"
	"strconv"

	"github.com/montanaflynn/stats"
)

const CLIENT_TIMEOUT = 500000000

/*
	Map the request with the response batch
	Compute the time taken for each request
	Computer the error rate
	Compute the throughput as successfully committed requests per second (doesn't include failed requests)
	Compute the latency
	Print the basic stats to the stdout and the logs to a file
*/

func (cl *Client) computeStats() {
	logFilePath := fmt.Sprintf("%s%d.txt", cl.logFilePath, cl.id)
	cl.debug(logFilePath, 0)
	f, err := os.Create(logFilePath) // log file
	
	if err != nil {
		panic("Error creating the output log file: " + err.Error())
	}
	defer f.Close()
	numTotalSentRequests := len(cl.sentRequests)
	numTotalResponses := len(cl.receivedResponses)

	var latencyList []int64 // contains the time duration spent requests in micro seconds

	for batchId, responseBatch := range cl.receivedResponses {
		sentBatch, ok := cl.sentRequests[batchId];
		if !ok {
			cl.debug("Response received for a batch that was not sent", 0)
			panic("should not happen")
		}

		batchLatency := responseBatch.time.Sub(sentBatch.time).Milliseconds()
		latencyList = append(latencyList, batchLatency)
		cl.printRequests(sentBatch.batch, sentBatch.time.Sub(cl.startTime).Milliseconds(), responseBatch.time.Sub(cl.startTime).Milliseconds(), f)
	}

	medianLatency, _ := stats.Median(cl.getFloat64List(latencyList))
	percentile99, _ := stats.Percentile(cl.getFloat64List(latencyList), 99.0) // tail latency
	duration := cl.testDuration
	errorRate := (numTotalSentRequests - numTotalResponses) * 100.0 / len(cl.sentRequests)
	requestsPerSecond := float64(numTotalResponses) / float64(duration)

	fmt.Printf("Total time := %d seconds\n", duration)
	fmt.Printf("Throughput (successfully committed requests) := %f requests per second\n", requestsPerSecond)
	fmt.Printf("Median Latency := %.2f milliseconds per request\n", medianLatency)
	fmt.Printf("99 pecentile latency := %.2f milliseconds per request\n", percentile99)
	fmt.Printf("Error Rate := %d \n", errorRate)
}

/*
	Converts int64[] to float64[]
*/

func (cl *Client) getFloat64List(list []int64) []float64 {
	var array []float64
	for i := 0; i < len(list); i++ {
		array = append(array, float64(list[i]))
	}
	return array
}

/*
	print a client request batch with arrival time and end time w.r.t test start time
*/

func (cl *Client) printRequests(messages *common.ClientBatch, startTime int64, endTime int64, f *os.File) {
	for i := 0; i < len(messages.Requests); i++ {
		_, _ = f.WriteString(messages.Requests[i].Command + "," + strconv.Itoa(int(startTime)) + "," + strconv.Itoa(int(endTime)) + "\n")
	}
}
