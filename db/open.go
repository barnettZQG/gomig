package db

import (
	"fmt"

	"github.com/barnettzqg/gomig/db/common"
	"github.com/barnettzqg/gomig/db/mysql"
	"github.com/barnettzqg/gomig/db/postgres"
)

func OpenReader(driverName string, conf *common.Config) (common.ReadCloser, error) {
	switch driverName {
	case "mysql":
		return mysql.OpenReader(conf)
	}

	return nil, fmt.Errorf("db: OpenReader: unknown driver type: %v", driverName)
}

func OpenFileWriter(driverName string, filename string) (common.WriteCloser, error) {
	switch driverName {
	case "postgres":
		return postgres.NewPostgresFileWriter(filename)
	}

	return nil, fmt.Errorf("db: OpenFileWriter: unknown driver type: %v", driverName)
}

func OpenWriter(driverName string, conf *common.Config) (common.WriteCloser, error) {
	switch driverName {
	case "postgres":
		return postgres.NewPostgresWriter(conf)
	}

	return nil, fmt.Errorf("db: OpenWriter: unknown driver type: %v", driverName)
}
