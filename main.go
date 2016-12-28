package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/rs/cors"
	"github.com/urfave/negroni"

	"github.com/yuuki/dynamond/lib/config"
	"github.com/yuuki/dynamond/lib/log"
	"github.com/yuuki/dynamond/lib/web"
)

const (
	DEFAULT_HOST   = "localhost"
	DEFAULT_PORT   = "8000"
	DEFAULT_CONFIG = "dynamond.conf"
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
	var (
		host     string
		port     string
		confPath string
		version  bool
		debug    bool
	)

	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() {
		fmt.Fprint(cli.errStream, helpText)
	}
	flags.StringVar(&host, "host", DEFAULT_HOST, "")
	flags.StringVar(&host, "H", DEFAULT_HOST, "")
	flags.StringVar(&port, "port", DEFAULT_PORT, "")
	flags.StringVar(&port, "P", DEFAULT_PORT, "")
	flags.StringVar(&confPath, "conf", DEFAULT_CONFIG, "")
	flags.StringVar(&confPath, "f", DEFAULT_CONFIG, "")
	flags.BoolVar(&version, "version", false, "")
	flags.BoolVar(&version, "v", false, "")
	flags.BoolVar(&debug, "debug", false, "")
	flags.BoolVar(&debug, "d", false, "")

	if err := flags.Parse(args[1:]); err != nil {
		return 1
	}
	log.SetDebug(debug)

	if version {
		fmt.Fprintf(cli.errStream, "%s version %s, build %s \n", Name, Version, GitCommit)
		return 0
	}

	if err := config.Load(confPath); err != nil {
		log.Printf("Failed to load the config file: %s", err)
		return 2
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/render", web.Render)

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
Usage: dynamond [options]

  dynamond is the DynamoDB-based TSDB API server.

Options:

  --config, -f         Config file

  --port, -P           Listen port

  --debug, -d          Run with debug print
`
