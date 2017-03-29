// +build integration

package render_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/yuuki/diamondb/test/integration/framework"
)

func TestQuery(t *testing.T) {
	resp, status := framework.Render("target=server1.loadavg5")
	if status != http.StatusOK {
		t.Errorf("status code shoud be 200: %v", status)
	}
	fmt.Printf("%v", resp)
}
