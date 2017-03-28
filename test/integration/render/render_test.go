// +build integration

package render_test

import (
	"fmt"
	"net/http"
	"testing"
)

func TestQuery(t *testing.T) {
	resp, err := http.Get("http://web:8000/render?target=server1.loadavg5")
	if err != nil {
		t.Errorf("shoud not raise error: %v", err)
	}
	fmt.Printf("%v", resp)
}
