package types

import (
	"math/rand"
	"time"
)

//const zeroTime = "0000-00-00 00:00:00"
const dateTimeLayout = "2006-01-02 15:03:04.000000"

type datetimeCol struct {
	col string
}

func NewDateTimeCol(col string) *datetimeCol {
	return &datetimeCol{col}
}

func (c *datetimeCol) ColType() string {
	return c.col
}

func (c *datetimeCol) Generate(r *rand.Rand) interface{} {
	//if r.Float64() < 0.05 {
	//	return zeroTime
	//} else {
	secondsPerYear := r.Int63n(3600 * 24 * 365)
	return time.Now().Add(-1 * time.Second * time.Duration(secondsPerYear)).Format(dateTimeLayout)
	//}
}

type timestampCol struct {
	col string
}

func NewTimestampCol(col string) *timestampCol {
	return &timestampCol{col}
}

func (c *timestampCol) ColType() string {
	return c.col
}

func (c *timestampCol) Generate(r *rand.Rand) interface{} {
	//if r.Float64() < 0.05 {
	//	return zeroTime
	//} else {
	secondsPerYear := r.Int63n(3600 * 24 * 365)
	return time.Now().Add(-1 * time.Second * time.Duration(secondsPerYear))
	//}
}
