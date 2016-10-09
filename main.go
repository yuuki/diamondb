package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/urfave/negroni"

	"github.com/yuuki/dynamond/handler"
	"github.com/yuuki/dynamond/log"
)

const (
	DEFAULT_PORT	= "8000"
)

func main() {
	var (
		port	string
		version bool
		debug	bool
	)

	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, helpText)
	}
	flags.StringVar(&port, "port", DEFAULT_PORT, "")
	flags.StringVar(&port, "P", DEFAULT_PORT, "")
	flags.BoolVar(&version, "version", false, "")
	flags.BoolVar(&version, "v", false, "")
	flags.BoolVar(&debug, "debug", false, "")
	flags.BoolVar(&debug, "d", false, "")

	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}
	log.IsDebug = debug

	mux := http.NewServeMux()
	mux.HandleFunc("/render", handler.Render)

	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.Use(negroni.NewLogger())
	n.UseHandler(mux)

	log.Error(http.ListenAndServe(":"+port, n))
}

var helpText = `
Usage: dynamond [options]

  dynamond is the DynamoDB-based TSDB API server.

Options:

  --port, -P           Listen port

  --debug, -d          Run with debug print
`
