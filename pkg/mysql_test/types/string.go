package types

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"unicode"
)

type stringCol struct {
	col    string
	length uint32
}

func NewStringCol(col string) *stringCol {
	length := oneParamFromType(col)
	return &stringCol{col, uint32(length)}
}

func NewTextCol(col string, bytes uint32) *stringCol {
	return &stringCol{col, bytes / 4}
}

func (c *stringCol) ColType() string {
	return c.col
}

var max = totalInRange(unicode.GraphicRanges)

func (c *stringCol) Generate(r *rand.Rand) interface{} {
	length := rand.Int63n(int64(c.length))
	ret := make([]rune, length, length)
	for i := int64(0); i < length; i++ {
		item := r.Intn(max)
		x := getItemInRangeTable(item)
		ret[i] = rune(x)
	}
	return string(ret)
}

type blobCol struct {
	col    string
	length uint32
}

func NewBlobCol(col string, length uint32) *blobCol {
	return &blobCol{col, length}
}

func (c *blobCol) ColType() string {
	return c.col
}

func (c *blobCol) Generate(r *rand.Rand) interface{} {
	length := rand.Int63n(int64(c.length))
	ret := make([]byte, length)
	r.Read(ret)
	return ret
}

type jsonCol struct {
	col string
}

// JSON column is not used right now. MySQL 5.7 has bug
// on checksum for json column.
func NewJSONCol(colType string) *jsonCol {
	return &jsonCol{colType}
}

func (c *jsonCol) ColType() string {
	return c.col
}

func (c *jsonCol) Generate(r *rand.Rand) interface{} {
	i := rand.Int()
	m := map[string]string{
		"id": fmt.Sprintf("%v", i),
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err.Error())
	}
	return string(b)
}

func getItemInRangeTable(n int) int {
	nPointer := n
	var picked int
	found := false

	for _, table := range unicode.GraphicRanges {
		if found == false {
			for _, r16 := range table.R16 {
				countInRange := int((r16.Hi-r16.Lo)/r16.Stride) + 1
				if nPointer <= countInRange-1 {
					picked = int(r16.Lo) + (int(r16.Stride) * nPointer)
					found = true
					break
				} else {
					nPointer -= countInRange
				}
			}

			if found == false {
				for _, r32 := range table.R32 {
					countInRange := int((r32.Hi-r32.Lo)/r32.Stride) + 1
					if nPointer <= countInRange-1 {
						picked = int(r32.Lo) + (int(r32.Stride) * nPointer)
						found = true
						break
					} else {
						nPointer -= countInRange
					}
				}
			}
		}
	}

	if found == true {
		return picked
	} else {
		return -1
	}
}

func totalInRange(tables []*unicode.RangeTable) int {
	total := 0
	for _, table := range tables {
		for _, r16 := range table.R16 {
			total += int((r16.Hi-r16.Lo)/r16.Stride) + 1
		}
		for _, r32 := range table.R32 {
			total += int((r32.Hi-r32.Lo)/r32.Stride) + 1
		}
	}
	return total
}
