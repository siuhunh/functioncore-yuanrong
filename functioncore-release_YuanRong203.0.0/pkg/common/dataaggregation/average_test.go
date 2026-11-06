package dataaggregation

import (
	"testing"
	"time"
)

func TestAverage_GetValue(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	{
		average := NewDataAggregation(AVERAGE, time.Second, 1, 1)
		average.AddSample(10, now)
		if average.GetValue(now.Add(time.Millisecond)) != 11 {
			t.Errorf("test average basic get value failed")
		}
	}
	{
		average := NewDataAggregation(AVERAGE, time.Second, 1, 3)
		average.AddSample(10, now)
		average.AddSample(13, now)
		average.AddSample(15, now)
		if average.GetValue(now.Add(time.Millisecond)) != 13 {
			t.Errorf("test average reach capacity get value failed")
		}
	}
	{
		average := NewDataAggregation(AVERAGE, time.Second, 1, 3)
		average.AddSample(10, now.Add(time.Millisecond))
		average.AddSample(13, now.Add(time.Millisecond))
		average.AddSample(15, now.Add(time.Millisecond))
		average.AddSample(15, now.Add(time.Second).Add(time.Millisecond))
		if average.GetValue(now.Add(time.Second).Add(time.Millisecond)) != 13 {
			t.Errorf("test average 2 time windows get value failed")
		}
	}
	{
		average := NewDataAggregation(AVERAGE, time.Second, 1, 2)
		average.AddSample(10, now.Add(time.Millisecond))
		average.AddSample(13, now.Add(time.Millisecond))
		average.AddSample(15, now.Add(time.Millisecond))
		average.AddSample(15, now.Add(time.Second).Add(time.Millisecond))
		average.AddSample(15, now.Add(time.Second).Add(time.Millisecond))
		if average.GetValue(now.Add(2*time.Second).Add(time.Millisecond)) != 13 {
			t.Errorf("test average 3 time windows get value failed")
		}
	}
}

func TestAverage_AddSamples(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	{
		average := NewDataAggregation(AVERAGE, time.Second, 1, 3)
		sample := []int64{10, 20, 30}
		average.AddSamples(sample, now)
		if average.GetValue(now.Add(time.Millisecond)) != 21 {
			t.Errorf("test average basic get value failed")
		}
	}
	{
		average := NewDataAggregation(AVERAGE, time.Second, 1, 5)
		sample := []int64{10, 20, 30, 40, 50}
		average.AddSamples(sample, now)
		if average.GetValue(now.Add(time.Millisecond)) != 31 {
			t.Errorf("test average basic get value failed")
		}
	}
	{
		average := NewDataAggregation(AVERAGE, time.Second, 1, 2)
		sample := []int64{10, 20, 30}
		average.AddSamples(sample, now.Add(time.Second))
		if average.GetValue(now.Add(time.Millisecond).Add(time.Millisecond)) != 21 {
			t.Errorf("test average reach capacity get value failed")
		}
	}
}
