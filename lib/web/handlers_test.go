package web

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"

	"github.com/yuuki/diamondb/lib/env"
	. "github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage"
)

func TestRenderHandler(t *testing.T) {
	fakefetcher := &storage.FakeFetcher{
		FakeFetch: func(name string, start, end time.Time) (SeriesSlice, error) {
			return SeriesSlice{
				NewSeries("server1.loadavg5", []float64{10.0, 11.0}, 1000, 60),
			}, nil
		},
	}
	r := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/render?target=server1.loadavg5", nil)
	if err != nil {
		panic(err)
	}

	RenderHandler(&env.Env{Fetcher: fakefetcher}).ServeHTTP(r, req)

	got, err := ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if r.Code == 200 {
		expected := "[{\"target\":\"server1.loadavg5\",\"datapoints\":[[10,1000],[11,1060]]}]"
		if diff := pretty.Compare(fmt.Sprintf("%s", got), expected); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	} else {
		t.Fatalf("response code should be 200")
	}

	if v := r.HeaderMap["Content-Type"][0]; v != "application/json" {
		t.Fatalf("response code should be not %s, but application/json", v)
	}
}
