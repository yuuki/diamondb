package web

import (
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"github.com/yuuki/diamondb/lib/log"
	"github.com/yuuki/diamondb/lib/model"
	"github.com/yuuki/diamondb/lib/query"
	"github.com/yuuki/diamondb/lib/timeparser"
)

const (
	DAYTIME = time.Duration(24*60*60) * time.Second
)

func Render(w http.ResponseWriter, r *http.Request) {
	until := time.Now().Round(time.Second)
	from := until.Add(-DAYTIME)

	if v := r.FormValue("from"); v != "" {
		t, err := timeparser.ParseAtTime(url.QueryEscape(v))
		if err != nil {
			log.Printf("%+v", err) // Print stack trace by pkg/errors
			BadRequest(w, errors.Cause(err).Error())
			return
		}
		from = t
	}
	if v := r.FormValue("until"); v != "" {
		t, err := timeparser.ParseAtTime(url.QueryEscape(v))
		if err != nil {
			log.Printf("%+v", err) // Print stack trace by pkg/errors
			BadRequest(w, errors.Cause(err).Error())
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
			log.Printf("%+v", err) // Print stack trace by pkg/errors
			switch err.(type) {
			case *query.ParserError, *query.UnsupportedFunctionError:
				BadRequest(w, errors.Cause(err).Error())
			default:
				ServerError(w, errors.Cause(err).Error())
			}
			return
		}
		for _, metric := range mList {
			vmList = append(vmList, metric.AsResponse())
		}
	}
	JSON(w, http.StatusOK, vmList)

	return
}
