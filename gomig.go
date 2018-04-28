package main

import (
	"os"

	"github.com/jessevdk/go-flags"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

//Options Options
type Options struct {
	/* verbosity level */
	Verbose []bool `short:"v" long:"verbose" description:"Verbose output"`
}

var options Options
var parser = flags.NewParser(&options, flags.Default)

func main() {
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}
}
