package main

import (
	"fmt"
	"io/ioutil"

	"github.com/barnettzqg/gomig/db/common"
	"github.com/go-yaml/yaml"
)

//DestinationConfig DestinationConfig
type DestinationConfig struct {
	File     string         `yaml:"file,omitempty"`
	Postgres *common.Config `yaml:"postgres,omitempty"`
}

//ProjectionConfig ProjectionConfig
type ProjectionConfig struct {
	Pk         []string          `yaml:"pk,omitempty"`
	Types      map[string]string `yaml:"column_types,omitempty"`
	Conditions string            `yaml:"destination_conditions,omitempty"`
	Body       string            `yaml:"body"`
	Engine     string            `yaml:"engine,omitempty"`
}

//Config Config
type Config struct {
	Mysql        *common.Config               `yaml:"mysql,omitempty"`
	Destination  *DestinationConfig           `yaml:"destination,omitempty"`
	Views        map[string]string            `yaml:"views,omitempty"`
	Projections  map[string]ProjectionConfig  `yaml:"projections,omitempty"`
	Tables       map[string]map[string]string `yaml:"tables,omitempty"`
	TableMap     map[string]string            `yaml:"table_map,omitempty"`
	SuppressData bool                         `yaml:"supress_data"`
	SuppressDdl  bool                         `yaml:"supress_ddl"`
	Truncate     bool                         `yaml:"force_truncate"`
	Merge        bool                         `yaml:"merge"`
	Timezone     bool                         `yaml:"timezone"`

	/* the included and excluded tables as both a map and a list, depending
	 * on what's most convenient. Note that the map version have last the
	 * ordering information. */
	OnlyTables     map[string]bool `yaml:"-"`
	OnlyTablesList []string        `yaml:"only_tables,omitempty"`

	ExcludeTables     map[string]bool `yaml:"-"`
	ExcludeTablesList []string        `yaml:"exclude_tables,omitempty"`
}

//LoadConfig LoadConfig
func LoadConfig(file string) (*Config, error) {
	yml, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var c Config
	err = yaml.Unmarshal(yml, &c)
	if err != nil {
		return nil, err
	}

	err = c.Validate()
	if err != nil {
		return nil, err
	}

	c.OnlyTables = stringSliceToSet(c.OnlyTablesList)
	c.ExcludeTables = stringSliceToSet(c.ExcludeTablesList)

	return &c, err
}

func stringSliceToSet(sl []string) map[string]bool {
	set := make(map[string]bool)
	for _, item := range sl {
		set[item] = true
	}
	return set
}

//Validate Validate
func (c *Config) Validate() error {
	if c.Mysql == nil {
		return fmt.Errorf("mysql section of config not present")
	}

	if c.Destination == nil {
		return fmt.Errorf("destination section of config not present or complete, %v", c)
	}

	if c.Destination.File == "" && c.Destination.Postgres == nil {
		return fmt.Errorf("either file or postgres has to be specified in "+
			"the destination field of the config file: %v", c)
	}

	return nil
}
