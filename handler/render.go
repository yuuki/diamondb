package handler

import (
	"net/http"
	"time"

	"github.com/yuuki/dynamond/log"
	"github.com/yuuki/dynamond/timeparser"
)

const (
	DAYTIME = time.Duration(24 * 60 * 60) * time.Second
)

func Render(w http.ResponseWriter, r *http.Request) {
	until := time.Now()
	from := until.Add(-DAYTIME)

	if v := r.FormValue("from"); v != "" {
		t, err := timeparser.ParseAtTime(v)
		if err != nil {
			BadRequest(w, err.Error())
			return
		}
		from = t
	}
	if v := r.FormValue("until"); v != "" {
		t, err := timeparser.ParseAtTime(v)
		if err != nil {
			BadRequest(w, err.Error())
			return
		}
		until = t
	}
	log.Debugf("from:%d until:%d", from.Unix(), until.Unix())

	return
}
