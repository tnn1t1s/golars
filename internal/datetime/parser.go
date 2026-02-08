package datetime

import (
	_ "fmt"
	_ "github.com/araddon/dateparse"
	_ "strconv"
	_ "strings"
	"sync"
	"time"
)

var tzCache = &sync.Map{}

func getTimezone(name string) (*time.Location, error) {
	panic("not implemented")

}

var commonFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05.999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05.999999",
	"2006-01-02T15:04:05.999",
	"2006-01-02T15:04:05",
	"2006-01-02",
	"2006/01/02",
	"01/02/2006",
	"02-Jan-2006",
	"02-Jan-2006 15:04:05",
	"Jan 02, 2006",
	"January 02, 2006",
	"2006-01-02 15:04:05 MST",
	"2006-01-02 15:04:05 -0700",
}

func ParseDateTime(value string) (DateTime, error) {
	panic("not implemented")

}

func ParseDateTimeWithFormat(value string, format string) (DateTime, error) {
	panic("not implemented")

}

func ParseDateTimeWithTimezone(value string, tz *time.Location) (DateTime, error) {
	panic("not implemented")

}

func ParseDate(value string) (Date, error) {
	panic("not implemented")

}

func ParseDateWithFormat(value string, format string) (Date, error) {
	panic("not implemented")

}

func ParseTime(value string) (Time, error) {
	panic("not implemented")

}

func ParseTimeWithFormat(value string, format string) (Time, error) {
	panic("not implemented")

}

func ParseDateTimeFromEpoch(epoch int64, unit TimeUnit) DateTime {
	panic("not implemented")

}

func convertToGoTimeFormat(format string) string {
	panic("not implemented")

}

func ParseDuration(s string) (Duration, error) {
	panic("not implemented")

}

func parsePeriodDuration(s string) (Duration, error) {
	panic("not implemented")

}
