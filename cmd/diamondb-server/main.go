package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/storage"
	"github.com/yuuki/diamondb/pkg/web"
)

// CLI is the command line object.
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
		log.Printf("Failed to load the config: %s\n", err)
		return 2
	}

	var (
		port    string
		version bool
	)

	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() {
		fmt.Fprint(cli.errStream, helpText)
	}
	flags.StringVar(&port, "port", config.DefaultPort, "")
	flags.StringVar(&port, "P", config.DefaultPort, "")
	flags.BoolVar(&version, "version", false, "")
	flags.BoolVar(&version, "v", false, "")

	if err := flags.Parse(args[1:]); err != nil {
		return 1
	}

	if version {
		fmt.Fprintf(cli.errStream, "%s version %s, build %s \n", Name, Version, GitCommit)
		return 0
	}

	store, err := storage.New()
	if err != nil {
		log.Printf("failed to start fetcher session. %s\n", err)
		return -1
	}

	log.Println("Initializing storage...")
	if err := store.Init(); err != nil {
		log.Printf("failed to initialize storage. %s\n", err)
		return -1
	}

	handler := web.New(&web.Option{
		Port:  port,
		Store: store,
	})
	go handler.Run()

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGTERM, syscall.SIGINT)
	s := <-sigch
	if err := handler.Shutdown(s); err != nil {
		log.Println(err)
		return 3
	}

	return 0
}

var helpText = `
Usage: diamondb-server [options]

  A Reliable, Scalable, Cloud-Based Time Series Database.

Options:
  --port, -P           Listen port
`
