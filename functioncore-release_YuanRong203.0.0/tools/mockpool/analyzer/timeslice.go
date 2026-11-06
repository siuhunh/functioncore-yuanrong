package analyzer

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// TimeSlice holds time.Duration values.
type TimeSlice []*CallResp

// Len -
func (ts TimeSlice) Len() int { return len(ts) }

// Less -
func (ts TimeSlice) Less(i, j int) bool { return int64(ts[i].duration) < int64(ts[j].duration) }

// Swap -
func (ts TimeSlice) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }

// String -
func (ts TimeSlice) String() string {
	var s strings.Builder
	for _, t := range ts {
		s.WriteString(t.String())
	}
	return s.String()
}

func (ts TimeSlice) cumulative() time.Duration {
	var total time.Duration
	for _, t := range ts {
		total += t.duration
	}

	return total
}

func (ts TimeSlice) hMean() time.Duration {
	var total float64

	for _, t := range ts {
		total += (1 / float64(t.duration))
	}

	return time.Duration(float64(ts.Len()) / total)
}

func (ts TimeSlice) avg() time.Duration {
	var total time.Duration
	for _, t := range ts {
		total += t.duration
	}
	return time.Duration(int(total) / ts.Len())
}

func (ts TimeSlice) p(p float64) time.Duration {
	return ts[int(float64(ts.Len())*p+0.5)-1].duration
}

func (ts TimeSlice) stdDev() time.Duration {
	m := ts.avg()
	s := 0.00

	for _, t := range ts {
		s += math.Pow(float64(m-t.duration), 2)
	}

	msq := s / float64(ts.Len())

	return time.Duration(math.Sqrt(msq))
}

func (ts TimeSlice) long5p() time.Duration {
	set := ts[int(float64(ts.Len())*0.95+0.5):]

	if len(set) <= 1 {
		return ts[ts.Len()-1].duration
	}

	var t time.Duration
	var i int
	for _, n := range set {
		t += n.duration
		i++
	}

	return time.Duration(int(t) / i)
}

func (ts TimeSlice) short5p() time.Duration {
	set := ts[:int(float64(ts.Len())*0.05+0.5)]

	if len(set) <= 1 {
		return ts[0].duration
	}

	var t time.Duration
	var i int
	for _, n := range set {
		t += n.duration
		i++
	}

	return time.Duration(int(t) / i)
}

func (ts TimeSlice) min() time.Duration {
	return ts[0].duration
}

func (ts TimeSlice) max() time.Duration {
	return ts[ts.Len()-1].duration
}

func (ts TimeSlice) srange() time.Duration {
	return ts.max() - ts.min()
}

// hgram returns a histogram of event durations in
// b bins, along with the bin size.
func (ts TimeSlice) hgram(b int) (*Histogram, time.Duration) {
	res := time.Duration(1000)
	// Interval is the time range / n bins
	interval := time.Duration(int64(ts.srange()) / int64(b))
	high := ts.min() + interval
	low := ts.min()
	max := ts.max()
	hgram := &Histogram{}
	pos := 1 // Bin position

	bstring := fmt.Sprintf("%s - %s", low/res*res, high/res*res)
	bin := map[string]uint64{}

	for _, v := range ts {
		// If v fits in the current bin,
		// increment the bin count
		if v.duration <= high {
			bin[bstring]++
		} else {
			// If not, prepare the next bin
			*hgram = append(*hgram, bin)
			bin = map[string]uint64{}

			// Update the high/low range values
			low = high + time.Nanosecond

			high += interval
			// if we're going into the
			// last bin, set high to max
			if pos == b-1 {
				high = max
			}

			bstring = fmt.Sprintf("%s - %s", low/res*res, high/res*res)

			// The value didn't fit in the previous
			// bin, so the new bin count should
			// be incremented
			bin[bstring]++

			pos++
		}
	}

	*hgram = append(*hgram, bin)

	return hgram, interval
}
