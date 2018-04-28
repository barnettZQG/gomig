package main

import (
	"fmt"

	"github.com/barnettzqg/gomig/db"
	"github.com/barnettzqg/gomig/db/common"
	"github.com/go-yaml/yaml"
)

//Command Command
type Command struct {
	/* config file */
	File string `short:"f" long:"file" description:"The path of the configuration file to use" default:"config.yml"`
}

func (x *Command) Execute(args []string) error {
	verbosity := len(options.Verbose)
	common.DBEXEC_VERBOSE = false

	conf := LoadConfigOrDie(x.File)

	haveError := false

	/* try connecting to the destination */
	if verbosity > 0 {
		rawDstParams, _ := yaml.Marshal(conf.Destination)
		dstParams := string(rawDstParams)
		fmt.Printf("destination:\n%v\n", IndentWith(dstParams, "  "))
	}
	fmt.Print("connecting...")
	if conf.Destination.File != "" {
		fmt.Println("IS A FILE")
	} else {
		writer, err := db.OpenWriter("postgres", conf.Destination.Postgres)
		if err != nil {
			fmt.Printf("ERROR (%v)\n", err)
			haveError = true
		} else {
			fmt.Println("OK")
			defer writer.Close()
		}
	}

	if haveError {
		return fmt.Errorf("could not connect to all databases")
	}

	return nil
}

func init() {
	var x Command
	parser.AddCommand("clear",
		"clear destination database",
		"clear destination database",
		&x)
}
