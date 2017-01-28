package series

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestNewDataPoint(t *testing.T) {
	p := NewDataPoint(1000, 0.1)
	if p.Timestamp() != 1000 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1000, p.Timestamp())
	}
	if p.Value() != 0.1 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 0.1, p.Value())
	}
}

var testDataPointMarshalJSONTests = []struct {
	desc     string
	point    *DataPoint
	expected string
}{
	{"not NaN", NewDataPoint(100, 10.5), "[10.5,100]"},
	{"NaN", NewDataPoint(100, math.NaN()), "[null,100]"},
}

func TestDataPointMarshalJSON(t *testing.T) {
	for _, tc := range testDataPointMarshalJSONTests {
		j, _ := json.Marshal(tc.point)
		got := fmt.Sprintf("%s", j)
		if diff := pretty.Compare(got, tc.expected); diff != "" {
			t.Fatalf("diff: (-actual +expected)\n%s", diff)
		}
	}
}

func TestDataPointsSort(t *testing.T) {
	points := DataPoints{
		NewDataPoint(10000, 0.2),
		NewDataPoint(1000, 0.1),
		NewDataPoint(1120, 0.2),
		NewDataPoint(1060, 0.3),
		NewDataPoint(900, 0.2),
	}
	points.Sort()
	expected := DataPoints{
		NewDataPoint(900, 0.2),
		NewDataPoint(1000, 0.1),
		NewDataPoint(1060, 0.3),
		NewDataPoint(1120, 0.2),
		NewDataPoint(10000, 0.2),
	}
	if diff := pretty.Compare(points, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}

func TestDataPointsDeduplicate(t *testing.T) {
	points := DataPoints{
		NewDataPoint(900, 0.5),
		NewDataPoint(900, 0.2),
		NewDataPoint(1000, 0.1),
		NewDataPoint(1060, 0.3),
		NewDataPoint(1120, 0.2),
		NewDataPoint(1120, 0.1),
	}
	points = points.Deduplicate()
	expected := DataPoints{
		NewDataPoint(900, 0.2),
		NewDataPoint(1000, 0.1),
		NewDataPoint(1060, 0.3),
		NewDataPoint(1120, 0.1),
	}
	if diff := pretty.Compare(points, expected); diff != "" {
		t.Fatalf("diff: (-actual +expected)\n%s", diff)
	}
}
