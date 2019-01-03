package mysql

import (
	"regexp"
	"strings"

	"github.com/pingcap/parser/ast"
)

var r = regexp.MustCompile(".+?(\\(.*)")

func RestoreCreateTblStmt(n *ast.CreateTableStmt) string {
	writer := &strings.Builder{}
	ctx := ast.NewRestoreCtx(ast.DefaultRestoreFlags, writer)
	ctx.WriteKeyWord("CREATE TABLE ")
	if !n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	_ = n.Table.Restore(ctx)
	ctx.WritePlain(" ")

	raw := n.Text()
	ctx.WritePlain(r.FindStringSubmatch(raw)[1])
	return writer.String()
}
