package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/rs/cors"
	"github.com/urfave/negroni"

	"github.com/yuuki/diamondb/lib/config"
	"github.com/yuuki/diamondb/lib/env"
	"github.com/yuuki/diamondb/lib/log"
	"github.com/yuuki/diamondb/lib/storage"
	"github.com/yuuki/diamondb/lib/web"
)

// CLI is the command line object
type CLI struct {
	// outStream and errStream are the stdout and stderr
	// to write message from the CLI.
	outStream, errStream io.Writer
}

func main() {
	cli := &CLI{outStream: os.Stdout, errStream: os.Stderr}
	os.Exit(cli.Run(os.Args))
}

// Run invokes the CLI with the given arguments.
func (cli *CLI) Run(args []string) int {
	if err := config.Load(); err != nil {
		log.Printf("Failed to load the config: %s", err)
		return 2
	}

	var (
		host    string
		port    string
		version bool
		debug   bool
	)

	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() {
		fmt.Fprint(cli.errStream, helpText)
	}
	flags.StringVar(&host, "host", config.Config.Host, "")
	flags.StringVar(&host, "H", config.Config.Host, "")
	flags.StringVar(&port, "port", config.Config.Port, "")
	flags.StringVar(&port, "P", config.Config.Port, "")
	flags.BoolVar(&version, "version", false, "")
	flags.BoolVar(&version, "v", false, "")
	flags.BoolVar(&debug, "debug", config.Config.Debug, "")
	flags.BoolVar(&debug, "d", config.Config.Debug, "")

	if err := flags.Parse(args[1:]); err != nil {
		return 1
	}
	log.SetDebug(debug)

	if version {
		fmt.Fprintf(cli.errStream, "%s version %s, build %s \n", Name, Version, GitCommit)
		return 0
	}

	e := &env.Env{Fetcher: storage.NewStore()}

	mux := http.NewServeMux()
	mux.Handle("/render", web.RenderHandler(e))
	mux.Handle("/datapoints", web.WriteHandler(e))

	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.Use(negroni.NewLogger())
	n.Use(cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Origin", "Accept", "Content-Type"},
	}))
	n.UseHandler(mux)

	log.Printf("Listening %s:%s ...", host, port)
	if err := http.ListenAndServe(":"+port, n); err != nil {
		log.Println(err)
		return 3
	}

	return 0
}

var helpText = `
Usage: diamondb [options]

  A Reliable, Scalable, Cloud-Based Time Series Database.

Options:
  --host, -H           Bind host

  --port, -P           Listen port

  --debug, -d          Run with debug print
`
