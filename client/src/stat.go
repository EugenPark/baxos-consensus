package src

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/montanaflynn/stats"
)

const WAIT_TIME = 5 // seconds

type ClientRequest struct {
	id       string
	command  string
	requestType string
	start    time.Time
	end      time.Time
}

func (r *ClientRequest) startTime() {
	r.start = time.Now()
}

func (r *ClientRequest) endTime() {
	r.end = time.Now()
}

func (r *ClientRequest) getLatency() time.Duration {
	return r.end.Sub(r.start)
}

func (r *ClientRequest) isWriteRequest() bool {
	return r.requestType == "write"
}

func (r *ClientRequest) isCompleted() bool {
	return !r.end.IsZero()
}

func getLatencies(requests map[string]*ClientRequest, filter func(*ClientRequest) bool) []float64 {
	var writeRequestLatency []float64
	for _, request := range requests {
		if filter(request) {
			writeRequestLatency = append(writeRequestLatency, float64(request.getLatency().Milliseconds()))
		}
	}
	return writeRequestLatency
}

func getNumberOfRequests(requests map[string]*ClientRequest, filter func(*ClientRequest) bool) int {
	var count int
	for _, request := range requests {
		if filter(request) {
			count++
		}
	}
	return count
}

func getRequestsPerSecond(numberOfRequests int, duration int) float64 {
	return float64(numberOfRequests) / float64(duration + WAIT_TIME)
}

func getErrorRate(totalRequests int, totalResponses int) int {
	if totalRequests == 0 {
		return 0
	}
	return (totalRequests - totalResponses) * 100 / totalRequests
}


/*
	Map the request with the response batch
	Compute the time taken for each request
	Computer the error rate
	Compute the throughput as successfully committed requests per second (doesn't include failed requests)
	Compute the latency
	Print the basic stats to the stdout and the logs to a file
*/

func (cl *Client) ComputeStats() {
	logFilePath := fmt.Sprintf("%s%d.txt", cl.logFilePath, cl.id)
	cl.debug(logFilePath, 0)
	f, err := os.Create(logFilePath) // log file
	
	if err != nil {
		panic("Error creating the output log file: " + err.Error())
	}
	defer f.Close()

	// lock the requests map for the duration of reading the stats
	cl.requestsMutex.Lock()
	defer cl.requestsMutex.Unlock()

	// compute the stats

	// latency
	latencyList := getLatencies(cl.requests, func(request *ClientRequest) bool {
		return request.isCompleted()
	})
	medianLatency, _ := stats.Median(latencyList)
	percentile99, _ := stats.Percentile(latencyList, 99.0) // tail latency

	writeLatencyList := getLatencies(cl.requests, func(request *ClientRequest) bool {
		return request.isCompleted() && request.isWriteRequest()
	})
	medianWriteLatency, _ := stats.Median(writeLatencyList)
	writePercentile99, _ := stats.Percentile(writeLatencyList, 99.0) // tail latency

	readLatencyList := getLatencies(cl.requests, func(request *ClientRequest) bool {
		return request.isCompleted() && !request.isWriteRequest()
	})
	medianReadLatency, _ := stats.Median(readLatencyList)
	readPercentile99, _ := stats.Percentile(readLatencyList, 99.0) // tail latency

	// throughput
	numTotalSentRequests := getNumberOfRequests(cl.requests, func(request *ClientRequest) bool {
		return true
	})
	numTotalResponses := getNumberOfRequests(cl.requests, func(request *ClientRequest) bool {
		return request.isCompleted()
	})
	requestsPerSecond := getRequestsPerSecond(numTotalResponses, cl.testDuration)
	errorRate := getErrorRate(numTotalSentRequests, numTotalResponses)

	numWriteRequests := getNumberOfRequests(cl.requests, func(request *ClientRequest) bool {
		return request.isWriteRequest()
	})
	numWriteResponses := getNumberOfRequests(cl.requests, func(request *ClientRequest) bool {
		return request.isCompleted() && request.isWriteRequest()
	})
	writeRequestsPerSecond := getRequestsPerSecond(numWriteResponses, cl.testDuration)
	writeErrorRate := getErrorRate(numWriteRequests, numWriteResponses)

	numReadRequests := getNumberOfRequests(cl.requests, func(request *ClientRequest) bool {
		return !request.isWriteRequest()
	})
	numReadResponses := getNumberOfRequests(cl.requests, func(request *ClientRequest) bool {
		return request.isCompleted() && !request.isWriteRequest()
	})
	readRequestsPerSecond := getRequestsPerSecond(numReadResponses, cl.testDuration)
	readErrorRate := getErrorRate(numReadRequests, numReadResponses)

	overallStatsPrint := fmt.Sprintf(
`---Overall Stats for Client %d---
Total time := %d seconds
Throughput (successfully committed requests) := %f requests per second
Median Latency := %.2f milliseconds per request
99 pecentile latency := %.2f milliseconds per request
Error Rate := %d
Total number of requests := %d
Total number of responses := %d
---Write Request Stats---
Throughput (successfully committed write requests) := %f requests per second
Median Latency for write requests := %.2f milliseconds per request
99 pecentile latency for write requests := %.2f milliseconds per request
Error Rate for write requests := %d
Total number of write requests sent := %d
Total number of write responses received := %d
---Read Request Stats---
Throughput (successfully committed read requests) := %f requests per second
Median Latency for read requests := %.2f milliseconds per request
99 pecentile latency for read requests := %.2f milliseconds per request
Error Rate for read requests := %d 
Total number of read requests sent := %d
Total number of read responses received := %d`,
		cl.id, cl.testDuration, requestsPerSecond, medianLatency, percentile99, errorRate, numTotalSentRequests, 
		numTotalResponses, writeRequestsPerSecond, medianWriteLatency, writePercentile99, writeErrorRate,
		numWriteRequests, numWriteResponses, readRequestsPerSecond, medianReadLatency, readPercentile99,
		readErrorRate, numReadRequests, numReadResponses)

	fmt.Println(overallStatsPrint)
	// write the stats to a file
	f.WriteString(overallStatsPrint)
	f.WriteString("\n---Request Details---\n")

	keys := make([]string, 0, len(cl.requests))
	for k := range cl.requests {
		keys = append(keys, k)
	}

	// sortIdFunc := func(i, j int) bool {
	// 	// Split the keys into their integer components
	// 	aParts := strings.Split(keys[i], ".")
	// 	bParts := strings.Split(keys[j], ".")

	// 	// Convert first part to integer
	// 	aPart1, _ := strconv.Atoi(aParts[0])
	// 	bPart1, _ := strconv.Atoi(bParts[0])

	// 	if aPart1 != bPart1 {
	// 		return aPart1 < bPart1
	// 	}

	// 	// Convert second part to integer
	// 	aPart2, _ := strconv.Atoi(aParts[1])
	// 	bPart2, _ := strconv.Atoi(bParts[1])

	// 	return aPart2 < bPart2
	// }
	
	sortTimeFunc := func(i, j int) bool {
		return cl.requests[keys[i]].end.Before(cl.requests[keys[j]].end)
	}

	sort.Slice(keys, sortTimeFunc)

	for _, key := range keys {
		request := cl.requests[key]
		f.WriteString(fmt.Sprintf("Id %s {\n\tCommand %s\n\tRequestType %s\n\tCompleted %t\n\tStartTime %d, EndTime %d => Duration in Milliseconds %d\n}\n\n", 
		                      request.id, request.command, request.requestType, request.isCompleted(), request.start.UnixNano(), request.end.UnixNano(), request.getLatency().Milliseconds()))
	}
}