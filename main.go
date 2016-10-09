package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/yuuki/dynamond/log"
)

const (
	DEFAULT_HOST	= "localhost"
	DEFAULT_PORT	= "8000"
)

func main() {
	var (
		host    string
		port	string
		version bool
		debug	bool
	)

	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, helpText)
	}
	flags.StringVar(&host, "host", DEFAULT_HOST, "")
	flags.StringVar(&host, "H", DEFAULT_HOST, "")
	flags.StringVar(&port, "port", DEFAULT_PORT, "")
	flags.StringVar(&port, "P", DEFAULT_PORT, "")
	flags.BoolVar(&version, "version", false, "")
	flags.BoolVar(&version, "v", false, "")
	flags.BoolVar(&debug, "debug", false, "")
	flags.BoolVar(&debug, "d", false, "")

	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}
	log.SetDebug(debug)

	n := NewServerHandler()

	log.Printf("Listening %s:%s ...", host, port)
	if err := http.ListenAndServe(":"+port, n); err != nil {
		log.Println(err)
		os.Exit(2)
	}
}

var helpText = `
Usage: dynamond [options]

  dynamond is the DynamoDB-based TSDB API server.

Options:

  --port, -P           Listen port

  --debug, -d          Run with debug print
`
