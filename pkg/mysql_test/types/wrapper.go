package types

import "math/rand"

type nullableCol struct {
	delegate ColumnValGenerator
}

func Nullable(c ColumnValGenerator) *nullableCol {
	return &nullableCol{c}
}

func (c *nullableCol) ColType() string {
	return c.delegate.ColType()
}

func (c *nullableCol) Generate(r *rand.Rand) interface{} {
	if r.Float32() < 0.2 {
		return nil
	} else {
		return c.delegate.Generate(r)
	}
}
