package analyzer

import (
	"fmt"
	"strings"
	"time"
)

// Histogram is a map["low-high duration"]count of events that
// fall within the low-high time duration range.
type Histogram []map[string]uint64

// Metrics holds the calculated outputs produced from a Tachymeter sample set.
type Metrics struct {
	Time struct { // All values under Time are selected entirely from events within the sample window
		Cumulative time.Duration // Cumulative time of all sampled events
		HMean      time.Duration // Event duration harmonic mean
		Avg        time.Duration // Event duration average
		P50        time.Duration // Event duration nth percentiles
		P75        time.Duration
		P95        time.Duration
		P99        time.Duration
		P999       time.Duration
		Short5p    time.Duration // Average of the shortest 5% event durations
		Long5p     time.Duration // Average of the longest 5% event durations
		Max        time.Duration // Highest event duration
		Min        time.Duration // Lowest event duration
		StdDev     time.Duration // Standard deviation
		Range      time.Duration // Event duration range (Max-Min)
	}
	Rate struct {
		// Per-second rate based on event duration avg. via Metrics.Cumulative / Metrics.Samples
		Second float64
		Wall   float64
	}
	Histogram        *Histogram    // Frequency distribution of event durations in len(Histogram) bins of HistogramBinSize
	HistogramBinSize time.Duration // The width of a histogram bin in time
	Samples          int           // Number of events included in the sample set
	Count            int           // Total number of events observed
	ErrorCount       int           // Total number of error events
	TopN             TimeSlice
}

// String satisfies the String interface.
func (m *Metrics) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d samples of %d events, %d fails,\n", m.Samples, m.Count, m.ErrorCount)
	fmt.Fprintf(&b, "Cumulative:     %s\n", m.Time.Cumulative)
	fmt.Fprintf(&b, "HMean:		     %s\n", m.Time.HMean)
	fmt.Fprintf(&b, "Avg.:		     %s\n", m.Time.Avg)
	fmt.Fprintf(&b, "p50: 		     %s\n", m.Time.P50)
	fmt.Fprintf(&b, "p75:		     %s\n", m.Time.P75)
	fmt.Fprintf(&b, "p95:		     %s\n", m.Time.P95)
	fmt.Fprintf(&b, "p99:		     %s\n", m.Time.P99)
	fmt.Fprintf(&b, "p999:		     %s\n", m.Time.P999)
	fmt.Fprintf(&b, "Short 5%%:	     %s\n", m.Time.Short5p)
	fmt.Fprintf(&b, "Long 5%%:	     %s\n", m.Time.Long5p)
	fmt.Fprintf(&b, "Max:		     %s\n", m.Time.Max)
	fmt.Fprintf(&b, "Min:		     %s\n", m.Time.Min)
	fmt.Fprintf(&b, "Range:		     %s\n", m.Time.Range)
	fmt.Fprintf(&b, "StdDev:	     %s\n", m.Time.StdDev)
	fmt.Fprintf(&b, "Rate/sec.:	     %.2f\n", m.Rate.Second)
	fmt.Fprintf(&b, "Wall Rate/sec.: %.2f\n", m.Rate.Wall)
	fmt.Fprintf(&b, "TopN: \n")
	fmt.Fprintf(&b, "%s", m.TopN)
	return b.String()
}
