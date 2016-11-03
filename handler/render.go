package handler

import (
	"net/http"
	"net/url"
	"time"

	"github.com/yuuki/dynamond/log"
	"github.com/yuuki/dynamond/model"
	"github.com/yuuki/dynamond/query"
	"github.com/yuuki/dynamond/timeparser"
)

const (
	DAYTIME = time.Duration(24 * 60 * 60) * time.Second
)

func Render(w http.ResponseWriter, r *http.Request) {
	until := time.Now()
	from := until.Add(-DAYTIME)

	if v := r.FormValue("from"); v != "" {
		t, err := timeparser.ParseAtTime(url.QueryEscape(v))
		if err != nil {
			BadRequest(w, err.Error())
			return
		}
		from = t
	}
	if v := r.FormValue("until"); v != "" {
		t, err := timeparser.ParseAtTime(url.QueryEscape(v))
		if err != nil {
			BadRequest(w, err.Error())
			return
		}
		until = t
	}
	log.Debugf("from:%d until:%d", from.Unix(), until.Unix())

	targets := r.Form["target"]
	if len(targets) < 1 {
		BadRequest(w, "no targets requested")
		return
	}

	vmList := make([]*model.ViewMetric, 0, len(targets))
	for _, target := range targets {
		mList, err := query.EvalTarget(target, from, until)
		if err != nil {
			BadRequest(w, err.Error())
			return
		}
		for _, metric := range mList {
			vmList = append(vmList, metric.AsResponse())
		}
	}
	JSON(w, http.StatusOK, vmList)

	return
}
