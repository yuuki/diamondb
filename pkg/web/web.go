package web

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/phyber/negroni-gzip/gzip"
	"github.com/pkg/errors"
	"github.com/rs/cors"
	"github.com/urfave/negroni"

	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/query"
	"github.com/yuuki/diamondb/pkg/storage"
	"github.com/yuuki/diamondb/pkg/timeparser"
)

const (
	// DayTime is one day period.
	DayTime = time.Duration(24*60*60) * time.Second
)

// Handler serves various HTTP endpoints of the Diamond server
type Handler struct {
	server *http.Server
	store  storage.ReadWriter
}

// Options for the web Handler.
type Option struct {
	Port  string
	Store storage.ReadWriter
}

// New initializes a new web Handler.
func New(o *Option) *Handler {
	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.Use(negroni.NewLogger())
	n.Use(gzip.Gzip(gzip.DefaultCompression))
	n.Use(cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Origin", "Accept", "Content-Type"},
	}))

	srv := &http.Server{Addr: ":" + o.Port, Handler: n}

	h := &Handler{
		server: srv,
		store:  o.Store,
	}

	mux := http.NewServeMux()
	mux.Handle("/ping", h.pingHandler())
	mux.Handle("/inspect", h.inspectHandler())
	mux.Handle("/render", http.TimeoutHandler(
		h.renderHandler(), config.Config.HTTPRenderTimeout, "/render timeout"),
	)
	mux.Handle("/datapoints", h.writeHandler())
	n.UseHandler(mux)

	return h
}

// Run serves the HTTP endpoints.
func (h *Handler) Run() {
	log.Printf("Listening on :%s\n", h.server.Addr)
	if err := h.server.ListenAndServe(); err != nil {
		log.Println(err)
	}
}

// Shutdown shoudowns the HTTP server.
func (h *Handler) Shutdown(sig os.Signal) error {
	log.Printf("Received %s gracefully shutdown...\n", sig)
	ctx, cancel := context.WithTimeout(context.Background(), config.Config.ShutdownTimeout)
	defer cancel()
	if err := h.server.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

// PingHandler returns a HTTP handler for the endpoint to ping storage.
func (h *Handler) pingHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := h.store.Ping(); err != nil {
			unavaliableError(w, errors.Cause(err).Error())
			return
		}
		ok(w, "PONG")
		return
	})
}

// InspectHandler returns a HTTP handler for the endpoint to inspect information.
func (h *Handler) inspectHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		renderJSON(w, http.StatusOK, config.Config)
		return
	})
}

// RenderHandler returns a HTTP handler for the endpoint to read data.
func (h *Handler) renderHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		until := time.Now().Round(time.Second)
		from := until.Add(-DayTime)

		if v := r.FormValue("from"); v != "" {
			t, err := timeparser.ParseAtTime(url.QueryEscape(v), config.Config.TimeZone)
			if err != nil {
				log.Println(err)
				badRequest(w, errors.Cause(err).Error())
				return
			}
			from = t
		}
		if v := r.FormValue("until"); v != "" {
			t, err := timeparser.ParseAtTime(url.QueryEscape(v), config.Config.TimeZone)
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

		seriesSlice, err := query.EvalTargets(h.store, targets, from, until)
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

// WriteRequest reprensents a request of /datapoints.
type WriteRequest struct {
	Metric *model.Metric `json:"metric"`
}

func (h *Handler) writeHandler() http.Handler {
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
		if err := h.store.InsertMetric(wr.Metric); err != nil {
			log.Printf("%+v", err) // Print stack trace by pkg/errors
			switch err.(type) {
			default:
				serverError(w, errors.Cause(err).Error())
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	})
}
