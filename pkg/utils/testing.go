package utils

import (
	"crypto/md5"
	"fmt"
	"io"
	"testing"
)

func TestCaseMd5Name(t *testing.T) string {
	h := md5.New()
	io.WriteString(h, t.Name())
	return fmt.Sprintf("%x", h.Sum(nil))
}
