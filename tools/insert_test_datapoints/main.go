package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/yuuki/diamondb/pkg/metric"
	"github.com/yuuki/diamondb/pkg/web"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func request(name string, timestamp int64, value float64, endpoint string) error {
	wr := &web.WriteRequest{
		Metric: &metric.Metric{
			Name: name,
			Datapoints: []*metric.Datapoint{
				&metric.Datapoint{Timestamp: timestamp, Value: value},
			},
		},
	}
	jsonData := new(bytes.Buffer)
	json.NewEncoder(jsonData).Encode(wr)

	resp, err := http.Post(fmt.Sprintf("%s/datapoints", endpoint), "application/json", jsonData)
	if err != nil {
		return err
	}
	if resp.StatusCode != 204 {
		log.Printf("http request error (%s,%d,%f) %s\n", name, timestamp, value, resp.Status)
		return nil
	}
	log.Printf("http success (%s,%d,%f)\n", name, timestamp, value)
	return nil
}

func write(name string, n int, step int, start int64, endpoint string, concurrency int) {
	rand.Seed(time.Now().UnixNano())

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()

		RETRY:
			select {
			case sem <- struct{}{}:
			default:
				time.Sleep(100 * time.Millisecond)
				goto RETRY
			}
			defer func() { <-sem }()

			timestamp := start + int64(step*i)
			value := rand.Float64() * 10.0
			if err := request(name, timestamp, value, endpoint); err != nil {
				log.Println(err)
			}
		}()
	}
	wg.Wait()
	return
}

func main() {
	var (
		name        string
		n           int
		step        int
		start       int64
		concurrency int
	)

	flags := flag.NewFlagSet("insert_test_datapoints", flag.ContinueOnError)
	flags.StringVar(&name, "name", "server1.loadavg5", "number of datapoints")
	flags.IntVar(&n, "num", 100, "number of datapoints")
	flags.IntVar(&step, "step", 60, "step")
	flags.Int64Var(&start, "start", 0, "start epoch time")
	flags.IntVar(&concurrency, "c", 1, "concurrency")

	if err := flags.Parse(os.Args[1:]); err != nil {
		log.Fatalln(err)
	}

	if l := len(flags.Args()); l != 1 {
		log.Fatalf("the number of arguments must be 1, but %d", l)
	}
	endpoint := flags.Arg(0)

	write(name, n, step, start, endpoint, concurrency)

	os.Exit(0)
}
