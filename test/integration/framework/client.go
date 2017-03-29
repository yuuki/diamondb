// +build integration

package framework

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const ENDPOINT = "http://web:8000"

type RenderResp struct {
	Target     string     `json:"target"`
	Datapoints Datapoints `json:"datapoints"`
}

type Datapoints []*Datapoint

type Datapoint struct {
	Timestamp int64
	Value     float64
}

func (d *Datapoint) UnmarshalJSON(data []byte) error {
	point := make([]interface{}, 2)
	if err := json.Unmarshal(data, &point); err != nil {
		return err
	}
	if t, ok := point[0].(int64); ok {
		d.Timestamp = t
	}
	if v, ok := point[1].(float64); ok {
		d.Value = v
	}
	return nil
}

func Render(query string) ([]*RenderResp, int) {
	resp, err := http.Get(
		fmt.Sprintf("http://web:8000/render?%s", query),
	)
	if err != nil {
		panic(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var data []*RenderResp
	if err := json.Unmarshal(body, &data); err != nil {
		panic(err)
	}

	return data, resp.StatusCode
}
