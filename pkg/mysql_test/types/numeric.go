package types

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/shopspring/decimal"
)

type integerColumn struct {
	colType string

	signed bool

	minSigned   int64
	maxSigned   int64
	maxUnsigned uint64
}

func NewIntegerColumn(colType string, signed bool, bytes int) *integerColumn {
	bits := uint64(bytes * 8)
	return &integerColumn{
		colType:     colType,
		signed:      signed,
		minSigned:   -(1 << (bits - 1)),
		maxSigned:   1<<(bits-1) - 1,
		maxUnsigned: 1<<bits - 1,
	}
}

func (c *integerColumn) ColType() string {
	return c.colType
}
func (c *integerColumn) Generate(r *rand.Rand) interface{} {
	if c.signed {
		return r.Int63n(c.maxSigned-c.minSigned) + c.minSigned
	} else if c.maxUnsigned < 1<<32 {
		return r.Uint32()
	} else {
		v := r.Uint64()
		for v > c.maxUnsigned {
			v = r.Uint64()
		}
		return v
	}
}

type decimalCol struct {
	colType   string
	precision int
	scale     int
}

func NewDecimalCol(colType string) *decimalCol {
	precision, scale := twoParamFromType(colType)
	return &decimalCol{colType: colType, precision: precision, scale: scale}
}

func (c *decimalCol) ColType() string {
	return c.colType
}
func (c *decimalCol) Generate(r *rand.Rand) interface{} {
	fractional := r.Int63n(int64(math.Pow10(c.scale)))
	integer := r.Float64() * math.Pow10(c.precision-c.scale)
	d, err := decimal.NewFromString(fmt.Sprintf("%.0f.%d", integer, fractional))
	if err != nil {
		panic(err)
	}
	return d
}

type floatCol struct {
	*decimalCol
}

func NewFloatCol(colType string) *floatCol {
	return &floatCol{NewDecimalCol(colType)}
}

func (c *floatCol) Generate(r *rand.Rand) interface{} {
	f, _ := c.decimalCol.Generate(r).(decimal.Decimal).Float64()
	return f
}

type bitCol struct {
	col string
	bit uint
}

func (c *bitCol) ColType() string {
	return c.col
}

func (c *bitCol) Generate(r *rand.Rand) interface{} {
	return r.Intn(1 << c.bit)
}

func NewBitCol(col string) *bitCol {
	b := oneParamFromType(col)
	return &bitCol{col, uint(b)}
}
