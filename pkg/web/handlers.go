package web

import (
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/env"
	"github.com/yuuki/diamondb/pkg/log"
	"github.com/yuuki/diamondb/pkg/metric"
	"github.com/yuuki/diamondb/pkg/query"
	"github.com/yuuki/diamondb/pkg/timeparser"
)

const (
	// DayTime is one day period.
	DayTime = time.Duration(24*60*60) * time.Second
)

// PingHandler returns a HTTP handler for the endpoint to ping storage.
func PingHandler(env *env.Env) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := env.ReadWriter.Ping(); err != nil {
			unavaliableError(w, errors.Cause(err).Error())
			return
		}
		ok(w, "PONG")
		return
	})
}

// InspectHandler returns a HTTP handler for the endpoint to inspect information.
func InspectHandler(env *env.Env) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		renderJSON(w, http.StatusOK, config.Config)
		return
	})
}

// RenderHandler returns a HTTP handler for the endpoint to read data.
func RenderHandler(env *env.Env) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		until := time.Now().Round(time.Second)
		from := until.Add(-DayTime)

		if v := r.FormValue("from"); v != "" {
			t, err := timeparser.ParseAtTime(url.QueryEscape(v))
			if err != nil {
				log.Println(err)
				badRequest(w, errors.Cause(err).Error())
				return
			}
			from = t
		}
		if v := r.FormValue("until"); v != "" {
			t, err := timeparser.ParseAtTime(url.QueryEscape(v))
			if err != nil {
				log.Println(err)
				badRequest(w, errors.Cause(err).Error())
				return
			}
			until = t
		}

		targets := r.Form["target"]
		if len(targets) < 1 {
			badRequest(w, "no targets requested")
			return
		}

		seriesSlice, err := query.EvalTargets(env.ReadWriter, targets, from, until)
		if err != nil {
			switch err := errors.Cause(err).(type) {
			case *query.ParserError, *query.UnsupportedFunctionError,
				*query.ArgumentError, *timeparser.TimeParserError:
				log.Println(err)
				badRequest(w, err.Error())
			default:
				log.Printf("%+v\n", err)
				serverError(w, err.Error())
			}
			return
		}
		renderJSON(w, http.StatusOK, seriesSlice)
	})
}

type WriteRequest struct {
	Metric *metric.Metric `json:"metric"`
}

func WriteHandler(env *env.Env) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var wr WriteRequest
		if r.Body == nil {
			badRequest(w, "No request body")
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&wr); err != nil {
			badRequest(w, err.Error())
			return
		}
		if err := env.ReadWriter.InsertMetric(wr.Metric); err != nil {
			log.Printf("%+v", err) // Print stack trace by pkg/errors
			switch err.(type) {
			default:
				serverError(w, errors.Cause(err).Error())
			}
			return
		}
		w.WriteHeader(204)
		return
	})
}
