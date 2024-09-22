package src

import (
	"fmt"
	"os"

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
	numTotalSentRequests := cl.getNumberOfSentWriteRequests()
	numTotalResponses := cl.getNumberOfReceivedWriteRequests()

	var latencyList []int64 // contains the time duration spent requests in micro seconds

	for _, requestTime := range cl.writeRequests {
		latencyList = append(latencyList, int64(requestTime.Duration().Milliseconds()))
		cl.printRequests(requestTime.command, requestTime.start.Sub(cl.startTime).Milliseconds(), requestTime.end.Sub(cl.startTime).Milliseconds(), f)
	}

	medianLatency, _ := stats.Median(cl.getFloat64List(latencyList))
	percentile99, _ := stats.Percentile(cl.getFloat64List(latencyList), 99.0) // tail latency
	duration := cl.testDuration
	// needs fixing since write request times is not the total number of received responses
	errorRate := (numTotalSentRequests - numTotalResponses) * 100.0 / numTotalSentRequests
	requestsPerSecond := float64(numTotalResponses) / float64(duration)

	fmt.Printf("Total time := %d seconds\n", duration)
	fmt.Printf("Throughput (successfully committed requests) := %f requests per second\n", requestsPerSecond)
	fmt.Printf("Median Latency := %.2f milliseconds per request\n", medianLatency)
	fmt.Printf("99 pecentile latency := %.2f milliseconds per request\n", percentile99)
	fmt.Printf("Error Rate := %d \n", errorRate)
}

func (cl *Client) getNumberOfReceivedWriteRequests() int {
	var count int

	for _, requestTime := range cl.writeRequests {
		if !requestTime.end.IsZero() {
			count++
		}
	}
	return count
}

func (cl *Client) getNumberOfSentWriteRequests() int {
	var count int
	for _, requestTime := range cl.writeRequests {
		if !requestTime.start.IsZero() {
			count++
		}
	}
	return count
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

func (cl *Client) printRequests(command string, startTime int64, endTime int64, f *os.File) {
	_, _ = f.WriteString(fmt.Sprintf("%s,%d,%d\n", command, startTime, endTime))
}
