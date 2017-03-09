package redis

import (
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/kylelemons/godebug/pretty"
	goredis "gopkg.in/redis.v5"

	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/model"
)

func TestNewRedis(t *testing.T) {
	tests := []struct {
		desc         string
		in           []string
		expectedType reflect.Type
	}{
		{
			"redis not cluster",
			[]string{"dummy:6379"},
			reflect.TypeOf((*goredis.Client)(nil)),
		},
		{
			"redis cluster",
			[]string{"dummy01:6379", "dummy02:6379"},
			reflect.TypeOf((*goredis.ClusterClient)(nil)),
		},
	}
	for _, tc := range tests {
		config.Config.RedisAddrs = tc.in
		r := New()
		if v := reflect.TypeOf(r.api()); v != tc.expectedType {
			t.Fatalf("desc: %s , Redis client type should be %s, not %s",
				tc.desc, tc.expectedType, v)
		}
	}
}

func TestPing(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	err = r.Ping()
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
}

func TestFetchSeriesMap(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	_, err = r.api().HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "160": "10.2", "220": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}
	_, err = r.api().HMSet("1m:server2.loadavg5", map[string]string{
		"100": "8.0", "160": "5.0", "220": "6.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	name := "server{1,2}.loadavg5"
	sm, err := r.Fetch(name, time.Unix(100, 0), time.Unix(1000, 0))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := model.SeriesMap{
		"server1.loadavg5": model.NewSeriesPoint("server1.loadavg5", model.DataPoints{
			model.NewDataPoint(100, 10.0),
			model.NewDataPoint(160, 10.2),
			model.NewDataPoint(220, 11.0),
		}, 60),
		"server2.loadavg5": model.NewSeriesPoint("server2.loadavg5", model.DataPoints{
			model.NewDataPoint(100, 8.0),
			model.NewDataPoint(160, 5.0),
			model.NewDataPoint(220, 6.0),
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
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	_, err = r.api().HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "130": "10.2", "160": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}
	_, err = r.api().HMSet("1m:server2.loadavg5", map[string]string{
		"100": "8.0", "130": "5.0", "160": "6.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	metrics, err := r.batchGet(&query{
		names: []string{"server1.loadavg5", "server2.loadavg5"},
		slot:  "1m",
		start: time.Unix(100, 0),
		end:   time.Unix(200, 0),
		step:  30,
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := model.SeriesMap{
		"server1.loadavg5": model.NewSeriesPoint("server1.loadavg5", model.DataPoints{
			model.NewDataPoint(100, 10.0),
			model.NewDataPoint(130, 10.2),
			model.NewDataPoint(160, 11.0),
		}, 30),
		"server2.loadavg5": model.NewSeriesPoint("server2.loadavg5", model.DataPoints{
			model.NewDataPoint(100, 8.0),
			model.NewDataPoint(130, 5.0),
			model.NewDataPoint(160, 6.0),
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
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	metrics, err := r.batchGet(&query{
		names: []string{"server1.loadavg5", "server2.loadavg5"},
		slot:  "1m",
		start: time.Unix(100, 0),
		end:   time.Unix(200, 0),
		step:  30,
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(metrics) != 0 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0, len(metrics))
	}
}

var testHGetAllToMapTests = []struct {
	desc     string
	name     string
	tsval    map[string]string
	query    *query
	expected *model.SeriesPoint
}{
	{
		"all datapoints within time range",
		"server1.loadavg5",
		map[string]string{"100": "10.0", "160": "11.0", "240": "12.0"},
		&query{
			names: []string{"server1.loadavg5"},
			start: time.Unix(100, 0),
			end:   time.Unix(240, 0),
			slot:  "1m",
			step:  60,
		},
		model.NewSeriesPoint(
			"server1.loadavg5", model.DataPoints{
				model.NewDataPoint(100, 10.0),
				model.NewDataPoint(160, 11.0),
				model.NewDataPoint(240, 12.0),
			}, 60,
		),
	},
	{
		"some datapoints out of time range",
		"server1.loadavg5",
		map[string]string{"40": "9.0", "100": "10.0", "160": "11.0", "240": "12.0", "300": "13.0"},
		&query{
			names: []string{"server1.loadavg5"},
			start: time.Unix(100, 0),
			end:   time.Unix(240, 0),
			slot:  "1m",
			step:  60,
		},
		model.NewSeriesPoint(
			"server1.loadavg5", model.DataPoints{
				model.NewDataPoint(100, 10.0),
				model.NewDataPoint(160, 11.0),
				model.NewDataPoint(240, 12.0),
			}, 60,
		),
	},
}

func TestHGetAllToMap(t *testing.T) {
	for _, tc := range testHGetAllToMapTests {
		got, err := hGetAllToMap(tc.name, tc.tsval, tc.query)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

func TestGet(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	_, err = r.api().HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "160": "10.2", "220": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	got, err := r.Get("1m", "server1.loadavg5")
	if err != nil {
		t.Fatalf("should not raise error: %s", err)
	}
	expected := map[int64]float64{
		100: 10.0,
		160: 10.2,
		220: 11.0,
	}
	if diff := pretty.Compare(got, expected); diff != "" {
		t.Fatalf("redis.Get(1m, server1.loadavg5); diff (-actual +expected)\n%s", diff)
	}
}

func TestLen(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	_, err = r.api().HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "160": "10.2", "220": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	n, err := r.Len("1m", "server1.loadavg5")
	if err != nil {
		panic(err)
	}

	if n != 3 {
		t.Fatalf("redis.Len(1m, server1.loadavg5) = %d; want 3\n", n)
	}
}

func TestMPut(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	expected := map[int64]float64{
		100: 10.0,
		160: 10.2,
		220: 11.0,
	}
	err = r.MPut("1m", "server1.loadavg5", expected)
	if err != nil {
		t.Fatalf("should not raise error: %s", err)
	}

	got, err := r.Get("1m", "server1.loadavg5")
	if err != nil {
		panic(err)
	}

	if diff := pretty.Compare(got, expected); diff != "" {
		t.Fatalf("redis.Get(1m, server1.loadavg5); diff (-actual +expected)\n%s", diff)
	}
}

func TestDelete(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Set mock
	config.Config.RedisAddrs = []string{s.Addr()}
	r := New()

	_, err = r.api().HMSet("1m:server1.loadavg5", map[string]string{
		"100": "10.0", "160": "10.2", "220": "11.0",
	}).Result()
	if err != nil {
		panic(err)
	}

	err = r.Delete("1m", "server1.loadavg5")
	if err != nil {
		t.Fatalf("shoud not raise error: %s", err)
	}

	got, err := r.Get("1m", "server1.loadavg5")
	if err != nil {
		panic(err)
	}
	if len(got) != 0 {
		t.Fatalf("the result of redis.Get should be 0, not %d after redis.Delete", len(got))
	}
}

func TestSelectTimeSlot(t *testing.T) {
	tests := []struct {
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

	for i, lc := range tests {
		slot, step := selectTimeSlot(lc.start, lc.end)
		if slot != lc.slot {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", lc.slot, slot, i)
		}
		if step != lc.step {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", lc.step, step, i)
		}
	}
}
