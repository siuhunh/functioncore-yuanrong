package dataaggregation

import (
	"testing"
	"time"
)

func TestCount_GetValue(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	{
		count := NewDataAggregation(COUNT, time.Second, 1, 0)
		count.AddSample(0, now)
		if count.GetValue(now) != 1 {
			t.Errorf("test count basic get value failed")
		}
	}
	{
		count := NewDataAggregation(COUNT, time.Second, 1, 0)
		count.AddSample(0, now.Add(time.Millisecond))
		if count.GetValue(now.Add(time.Second).Add(time.Millisecond)) != 1 {
			t.Errorf("test count basic get value failed")
		}
	}
	{
		count := NewDataAggregation(COUNT, time.Second, 1, 0)
		count.AddSample(0, now.Add(time.Millisecond))
		count.AddSample(0, now.Add(time.Millisecond))
		count.AddSample(0, now.Add(time.Second).Add(time.Millisecond))
		if count.GetValue(now.Add(time.Second).Add(time.Millisecond)) != 2 {
			t.Errorf("test compatible count get value failed")
		}
	}
	{
		count := NewDataAggregation(COUNT, time.Second, 1, 0)
		count.AddSample(0, now.Add(time.Millisecond))
		count.AddSample(0, now.Add(time.Millisecond))
		count.AddSample(0, now.Add(time.Second).Add(time.Millisecond))
		if count.GetValue(now.Add(2*time.Second).Add(time.Millisecond)) != 1 {
			t.Errorf("test compatible count get value failed")
		}
	}
}
