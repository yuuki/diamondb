// +build integration

package writer

import (
	"net/http"
	"testing"

	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/test/integration/framework"
)

func TestWrite(t *testing.T) {
	status := framework.Write(&model.Metric{
		Name: "server1.loadavg5",
		Datapoints: []*model.Datapoint{
			{Timestamp: 0, Value: 1.0},
			{Timestamp: 60, Value: 1.1},
		},
	})
	if status != http.StatusNoContent {
		t.Errorf("status code shoud be 204: %v", status)
	}
}
