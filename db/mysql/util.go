package mysql

import (
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/barnettzqg/gomig/db/common"
)

func MysqlToGenericType(mysqlType string) *common.Type {
	rt := mysqlType
	switch {
	case rt == "set":
		return common.SetType()
	case rt == "date":
		return common.DateType()
	case rt == "time":
		return common.TimeType()
	case rt == "datetime", rt == "timestamp":
		return common.TimestampType()
	case strings.Contains(rt, "float"):
		return common.FloatType()
	case strings.Contains(rt, "double"):
		return common.DoubleType()
	case strings.Contains(rt, "numeric"), strings.Contains(rt, "decimal"):
		scale, precision := ExtractPrecisionAndScale(rt)
		return common.NumericType(scale, precision)
	case strings.Contains(rt, "tinyint"):
		return common.BoolType()
	case rt == "smallint", rt == "year":
		return common.IntType(common.TypeSmall)
	case rt == "bigint", rt == "int unsigned":
		return common.IntType(common.TypeLarge)
	case rt == "bigint unsigned":
		return common.IntType(common.TypeHuge)
	case strings.Contains(rt, "int"), rt == "smallint unsigned":
		return common.IntType(common.TypeNormal)
	case strings.Contains(rt, "blob"), strings.Contains(rt, "binary"):
		return common.BlobType()
	case strings.HasPrefix(rt, "char"):
		t := common.PaddedTextType()
		t.Max = ExtractLength(rt)
		return t
	case strings.Contains(rt, "varchar"), strings.Contains(rt, "text"):
		t := common.TextType()
		t.Max = ExtractLength(rt)
		return t
	case strings.HasPrefix(rt, "bit") && rt != "bit":
		return common.BitType(ExtractLength(rt))
	case rt == "bit", rt == "bit(1)", rt == "tinyint(1)", rt == "tinyint(1) unsigned":
		return common.BoolType()
	default:
		log.Println("WARNING: mysql: encountered an unknown type, ", rt)
		return common.SimpleType(rt)
	}
}

/* returns 0 if no length could be determined */
func ExtractLength(mysqlType string) uint {
	/* matches should be: [mysqlType, length] */
	matches := regexp.MustCompile(`\w+\((\d+)\)`).FindStringSubmatch(mysqlType)

	if len(matches) != 2 {
		return 0
	}

	i, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0
	}

	return uint(i)
}

/* returns a precision, scale tuple */
func ExtractPrecisionAndScale(mysqlType string) (uint, uint) {
	/* we should get something like: TYPE(precision, scale) */
	/* matches should be: [mysqlType, precision, scale] */
	matches := regexp.MustCompile(`\w+\(\s*(\d+)\s*,\s*(\d+)\s*\)`).FindStringSubmatch(mysqlType)

	if len(matches) != 3 {
		return 0, 0
	}

	precision, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, 0
	}
	scale, err := strconv.Atoi(matches[2])

	return uint(precision), uint(scale)
}
