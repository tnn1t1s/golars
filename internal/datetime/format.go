package datetime

import (
	_ "fmt"
	_ "strings"
	"time"
)

func (dt DateTime) Format(layout string) string {
	panic("not implemented")

}

func (dt DateTime) ISOFormat() string {
	panic("not implemented")

}

func (dt DateTime) String() string {
	panic("not implemented")

}

func (d Date) Format(layout string) string {
	panic("not implemented")

}

func (d Date) ISOFormat() string {
	panic("not implemented")

}

func (d Date) String() string {
	panic("not implemented")

}

func (t Time) Format(layout string) string {
	panic("not implemented")

}

func (t Time) ISOFormat() string {
	panic("not implemented")

}

func (t Time) String() string {
	panic("not implemented")

}

func (tu TimeUnit) ToDuration() time.Duration {
	panic("not implemented")

}

func FormatWithPolarsStyle(dt DateTime, format string) string {
	panic("not implemented")

}

func convertPolarsToGoFormat(format string) string {
	panic("not implemented")

}

func FormatDuration(d Duration) string {
	panic("not implemented")

}

func (d Duration) String() string {
	panic("not implemented")

}
