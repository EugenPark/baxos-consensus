package src

import (
	"fmt"
	"os"
	"time"

	"github.com/montanaflynn/stats"
)

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
	return float64(numberOfRequests) / float64(duration)
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

func (cl *Client) computeStats() {
	logFilePath := fmt.Sprintf("%s%d.txt", cl.logFilePath, cl.id)
	cl.debug(logFilePath, 0)
	f, err := os.Create(logFilePath) // log file
	
	if err != nil {
		panic("Error creating the output log file: " + err.Error())
	}
	defer f.Close()

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

	fmt.Printf("---Overall Stats for Client %d---\n", cl.id)
	fmt.Printf("Total time := %d seconds\n", cl.testDuration)
	fmt.Printf("Throughput (successfully committed requests) := %f requests per second\n", requestsPerSecond)
	fmt.Printf("Median Latency := %.2f milliseconds per request\n", medianLatency)
	fmt.Printf("99 pecentile latency := %.2f milliseconds per request\n", percentile99)
	fmt.Printf("Error Rate := %d \n", errorRate)
	fmt.Printf("Total number of requests := %d\n", numTotalSentRequests)
	fmt.Printf("Total number of responses := %d\n", numTotalResponses)
	fmt.Printf("---Write Request Stats---\n")
	fmt.Printf("Throughput (successfully committed write requests) := %f requests per second\n", writeRequestsPerSecond)
	fmt.Printf("Median Latency for write requests := %.2f milliseconds per request\n", medianWriteLatency)
	fmt.Printf("99 pecentile latency for write requests := %.2f milliseconds per request\n", writePercentile99)
	fmt.Printf("Error Rate for write requests := %d \n", writeErrorRate)
	fmt.Printf("Total number of write requests sent := %d\n", numWriteRequests)
	fmt.Printf("Total number of write responses received := %d\n", numWriteResponses)
	fmt.Printf("---Read Request Stats---\n")
	fmt.Printf("Throughput (successfully committed read requests) := %f requests per second\n", readRequestsPerSecond)
	fmt.Printf("Median Latency for read requests := %.2f milliseconds per request\n", medianReadLatency)
	fmt.Printf("99 pecentile latency for read requests := %.2f milliseconds per request\n", readPercentile99)
	fmt.Printf("Error Rate for read requests := %d \n", readErrorRate)
	fmt.Printf("Total number of read requests sent := %d\n", numReadRequests)
	fmt.Printf("Total number of read responses received := %d\n", numReadResponses)

	// write the stats to a file
	f.WriteString(fmt.Sprintf("---Overall Stats for Client %d---\n", cl.id))
	f.WriteString(fmt.Sprintf("Total time := %d seconds\n", cl.testDuration))
	f.WriteString(fmt.Sprintf("Throughput (successfully committed requests) := %f requests per second\n", requestsPerSecond))
	f.WriteString(fmt.Sprintf("Median Latency := %.2f milliseconds per request\n", medianLatency))
	f.WriteString(fmt.Sprintf("99 pecentile latency := %.2f milliseconds per request\n", percentile99))
	f.WriteString(fmt.Sprintf("Error Rate := %d \n", errorRate))
	f.WriteString(fmt.Sprintf("Total number of requests := %d\n", numTotalSentRequests))
	f.WriteString(fmt.Sprintf("Total number of responses := %d\n", numTotalResponses))
	f.WriteString("---Write Request Stats---\n")
	f.WriteString(fmt.Sprintf("Throughput (successfully committed write requests) := %f requests per second\n", writeRequestsPerSecond))
	f.WriteString(fmt.Sprintf("Median Latency for write requests := %.2f milliseconds per request\n", medianWriteLatency))
	f.WriteString(fmt.Sprintf("99 pecentile latency for write requests := %.2f milliseconds per request\n", writePercentile99))
	f.WriteString(fmt.Sprintf("Error Rate for write requests := %d \n", writeErrorRate))
	f.WriteString(fmt.Sprintf("Total number of write requests sent := %d\n", numWriteRequests))
	f.WriteString(fmt.Sprintf("Total number of write responses received := %d\n", numWriteResponses))
	f.WriteString("---Read Request Stats---\n")
	f.WriteString(fmt.Sprintf("Throughput (successfully committed read requests) := %f requests per second\n", readRequestsPerSecond))
	f.WriteString(fmt.Sprintf("Median Latency for read requests := %.2f milliseconds per request\n", medianReadLatency))
	f.WriteString(fmt.Sprintf("99 pecentile latency for read requests := %.2f milliseconds per request\n", readPercentile99))
	f.WriteString(fmt.Sprintf("Error Rate for read requests := %d \n", readErrorRate))
	f.WriteString(fmt.Sprintf("Total number of read requests sent := %d\n", numReadRequests))
	f.WriteString(fmt.Sprintf("Total number of read responses received := %d\n", numReadResponses))
	f.WriteString("\n---Request Details---\n")

	for _, request := range cl.requests {
		f.WriteString(fmt.Sprintf("Id %s {\n\tCommand %s\n\tRequestType %s\n\tCompleted %t\n\tStartTime %d, EndTime %d => Duration in Milliseconds %d\n}\n\n", 
		                      request.id, request.command, request.requestType, request.isCompleted(), request.start.UnixNano(), request.end.UnixNano(), request.getLatency().Milliseconds()))
	}
}