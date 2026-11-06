package analyzer

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// CallResp -
type CallResp struct {
	traceID  string
	duration time.Duration
	resp     fmt.Stringer
}

// String -
func (resp *CallResp) String() string {
	var s strings.Builder
	fmt.Fprintln(&s, "----------------")
	fmt.Fprintf(&s, "traceID: %s\n", resp.traceID)
	fmt.Fprintf(&s, "duration: %v\n", resp.duration)
	fmt.Fprintln(&s, resp.resp.String())
	if gostringer, ok := resp.resp.(fmt.GoStringer); ok {
		fmt.Fprintln(&s, gostringer.GoString())
	}
	return s.String()
}

// Record -
type Record struct {
	errorCount int
	count      int
	size       int
	times      TimeSlice
}

// ResponseAnalyzer -
type ResponseAnalyzer struct {
	outputEachResult  bool
	outputErrorResult bool
	topN              int
	records           []*Record
	outputCh          chan *CallResp
}

// NewResponseAnalyzer -
func NewResponseAnalyzer(
	outputEachResult bool,
	outputErrorResult bool,
	topN int,
	concurrency int) *ResponseAnalyzer {

	analyzer := &ResponseAnalyzer{
		outputEachResult:  outputEachResult,
		outputErrorResult: outputErrorResult,
		topN:              topN,
		records:           make([]*Record, concurrency),
		outputCh:          make(chan *CallResp),
	}
	go analyzer.echoOutput()
	return analyzer
}

// NewLocal -
func (a *ResponseAnalyzer) NewLocal(id int) {
	size := 2 << 13
	record := &Record{
		size:  size,
		times: make(TimeSlice, size),
	}
	a.records[id] = record
}

// AddRecord -
func (a *ResponseAnalyzer) AddRecord(id int, duration time.Duration, traceID string, resp fmt.Stringer, isError bool) {
	if a.outputEachResult {
		a.outputResp(duration, resp)
	}

	record := a.records[id]
	if isError {
		if !a.outputEachResult && a.outputErrorResult {
			a.outputResp(duration, resp)
		}
		record.errorCount++
	}
	record.times[record.count&(record.size-1)] = &CallResp{traceID, duration, resp}
	record.count++
}

func (a *ResponseAnalyzer) outputResp(duration time.Duration, resp fmt.Stringer) {
	a.outputCh <- &CallResp{
		duration: duration,
		resp:     resp,
	}
}

func (a *ResponseAnalyzer) echoOutput() {
	for resp := range a.outputCh {
		fmt.Println(resp)
	}
}

// Calc -
func (a *ResponseAnalyzer) Calc(wallTime time.Duration) *Metrics {
	metrics := &Metrics{}
	times := make(TimeSlice, 0)

	for _, record := range a.records {
		metrics.Count += record.count
		metrics.ErrorCount += record.errorCount
		size := min(record.count, record.size)
		times = append(times, record.times[:size]...)
		metrics.Samples += size
	}

	sort.Sort(times)

	metrics.Time.Cumulative = times.cumulative()
	metrics.Rate.Second = float64(metrics.Samples) / float64(metrics.Time.Cumulative) * 1e9
	metrics.Rate.Wall = float64(metrics.Count) / float64(wallTime) * 1e9

	metrics.Time.Avg = times.avg()
	metrics.Time.HMean = times.hMean()
	metrics.Time.P50 = times[times.Len()/2].duration
	metrics.Time.P75 = times.p(0.75)
	metrics.Time.P95 = times.p(0.95)
	metrics.Time.P99 = times.p(0.99)
	metrics.Time.P999 = times.p(0.999)
	metrics.Time.Long5p = times.long5p()
	metrics.Time.Short5p = times.short5p()
	metrics.Time.Min = times.min()
	metrics.Time.Max = times.max()
	metrics.Time.Range = times.srange()
	metrics.Time.StdDev = times.stdDev()

	metrics.Histogram, metrics.HistogramBinSize = times.hgram(10)

	n := min(metrics.Samples, a.topN)
	metrics.TopN = times[len(times)-n:]

	return metrics
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
