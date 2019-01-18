package mysql

import (
	"regexp"
	"strings"

	"github.com/pingcap/parser/ast"
)

var expCreateTable = regexp.MustCompile("(?mi)^CREATE\\sTABLE(?:\\sIF\\sNOT\\sEXISTS)?\\s\\x60?.*?\\x60?\\.?\\x60?[^\\x60\\.]+?\\x60?(\\s[\\s\\S]*)")
var expAlterTable = regexp.MustCompile("(?im)^ALTER\\sTABLE\\s.*?`?.*?`?\\.?`?[^`.]+?`?\\s([\\s\\S]*)")

func RestoreCreateTblStmt(n *ast.CreateTableStmt) string {
	writer := &strings.Builder{}
	ctx := ast.NewRestoreCtx(ast.DefaultRestoreFlags, writer)
	ctx.WriteKeyWord("CREATE TABLE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	_ = n.Table.Restore(ctx)

	raw := n.Text()
	ctx.WritePlain(expCreateTable.FindStringSubmatch(raw)[1])
	return writer.String()
}

func RestoreAlterTblStmt(n *ast.AlterTableStmt) string {
	writer := &strings.Builder{}
	ctx := ast.NewRestoreCtx(ast.DefaultRestoreFlags, writer)
	ctx.WriteKeyWord("ALTER TABLE ")
	_ = n.Table.Restore(ctx)
	ctx.WritePlain(" ")

	raw := n.Text()
	ctx.WritePlain(expAlterTable.FindStringSubmatch(raw)[1])
	return writer.String()
}
