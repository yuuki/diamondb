package main

import (
	"net/http"

	"github.com/urfave/negroni"

	"github.com/yuuki/dynamond/handler"
)

func NewServerHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/render", handler.Render)

	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.Use(negroni.NewLogger())
	n.UseHandler(mux)

	return n
}
