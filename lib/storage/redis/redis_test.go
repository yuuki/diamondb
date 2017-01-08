package redis

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/kylelemons/godebug/pretty"
	"github.com/yuuki/diamondb/lib/series"
	redis "gopkg.in/redis.v5"
)

func TestFetchSeriesMap(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	c := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	client = c

	_, err = c.HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "160": "10.2", "220": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}
	_, err = c.HMSet("1m:server2.loadavg5", map[string]string{
		"100": "8.0", "160": "5.0", "220": "6.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	name := "server{1,2}.loadavg5"
	sm, err := FetchSeriesMap(name, time.Unix(100, 0), time.Unix(1000, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := series.SeriesMap{
		"server1.loadavg5": series.NewSeriesPoint("server1.loadavg5", series.DataPoints{
			series.NewDataPoint(100, 10.0),
			series.NewDataPoint(160, 10.2),
			series.NewDataPoint(220, 11.0),
		}, 60),
		"server2.loadavg5": series.NewSeriesPoint("server2.loadavg5", series.DataPoints{
			series.NewDataPoint(100, 8.0),
			series.NewDataPoint(160, 5.0),
			series.NewDataPoint(220, 6.0),
		}, 60),
	}
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestBatchGet(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	c := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	client = c

	_, err = c.HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "130": "10.2", "160": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}
	_, err = c.HMSet("1m:server2.loadavg5", map[string]string{
		"100": "8.0", "130": "5.0", "160": "6.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	names := []string{"server1.loadavg5", "server2.loadavg5"}
	metrics, err := batchGet("1m", names, 30)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := series.SeriesMap{
		"server1.loadavg5": series.NewSeriesPoint("server1.loadavg5", series.DataPoints{
			series.NewDataPoint(100, 10.0),
			series.NewDataPoint(130, 10.2),
			series.NewDataPoint(160, 11.0),
		}, 30),
		"server2.loadavg5": series.NewSeriesPoint("server2.loadavg5", series.DataPoints{
			series.NewDataPoint(100, 8.0),
			series.NewDataPoint(130, 5.0),
			series.NewDataPoint(160, 6.0),
		}, 30),
	}
	if diff := pretty.Compare(metrics, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestBatchGet_Empty(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	c := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	client = c

	names := []string{"server1.loadavg5", "server2.loadavg5"}
	metrics, err := batchGet("1m", names, 30)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(metrics) != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, len(metrics))
	}
}

func TestConcurrentBatchGet(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	c := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	client = c

	_, err = c.HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "130": "10.2", "160": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}
	_, err = c.HMSet("1m:server2.loadavg5", map[string]string{
		"100": "8.0", "130": "5.0", "160": "6.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	names := []string{"server1.loadavg5", "server2.loadavg5"}
	ch := make(chan interface{})

	concurrentBatchGet("1m", names, 30, ch)

	ret := <-ch
	sm := ret.(series.SeriesMap)
	expected := series.SeriesMap{
		"server1.loadavg5": series.NewSeriesPoint("server1.loadavg5", series.DataPoints{
			series.NewDataPoint(100, 10.0),
			series.NewDataPoint(130, 10.2),
			series.NewDataPoint(160, 11.0),
		}, 30),
		"server2.loadavg5": series.NewSeriesPoint("server2.loadavg5", series.DataPoints{
			series.NewDataPoint(100, 8.0),
			series.NewDataPoint(130, 5.0),
			series.NewDataPoint(160, 6.0),
		}, 30),
	}
	if diff := pretty.Compare(sm, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

var selectTimeSlotTests = []struct {
	start time.Time
	end   time.Time
	slot  string
	step  int
}{
	{time.Unix(100, 0), time.Unix(6000, 0), "1m", 60},
	{time.Unix(10000, 0), time.Unix(100000, 0), "5m", 300},
	{time.Unix(100000, 0), time.Unix(1000000, 0), "1h", 3600},
	{time.Unix(1000000, 0), time.Unix(100000000, 0), "1d", 86400},
}

func TestSelectTimeSlot(t *testing.T) {
	for i, lc := range selectTimeSlotTests {
		slot, step := selectTimeSlot(lc.start, lc.end)
		if slot != lc.slot {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", lc.slot, slot, i)
		}
		if step != lc.step {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", lc.step, step, i)
		}
	}
}
