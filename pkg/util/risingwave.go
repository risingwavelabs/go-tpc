package util

import (
	"fmt"
	"regexp"
)

func ConvertToRisingWaveDDL(query string) string {
	fmt.Println(query)
	query = regexp.MustCompile("(?i)varchar\\(\\d+\\)").ReplaceAllString(query, "VARCHAR")
	query = regexp.MustCompile("(?i)numeric\\(.*?\\)").ReplaceAllString(query, "NUMERIC")
	query = regexp.MustCompile("(?i)decimal\\(.*?\\)").ReplaceAllString(query, "DECIMAL")
	query = regexp.MustCompile("(?i)char\\(\\d+\\)").ReplaceAllString(query, "VARCHAR")
	query = regexp.MustCompile("(?i) not null").ReplaceAllString(query, "")
	fmt.Println(query)
	return query
}
