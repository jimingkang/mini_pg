

#include <stdio.h>
#include "sql_stmt.h"
#include "sqliteInt.h"  // 定义了 struct Expr 和 struct Select

MiniExpr* convertExpr( Expr* sqlite_expr) {
    MiniExpr* out = (MiniExpr*)malloc(sizeof(MiniExpr));
    if (!out) return NULL;

    switch (sqlite_expr->op) {
        case TK_STRING:
        case TK_INTEGER:
        case TK_FLOAT:
            out->type = EXPR_LITERAL;
            out->value = strdup(sqlite_expr->u.zToken);
            break;

        case TK_ID:
        case TK_COLUMN:
            out->type = EXPR_COLUMN;
            out->value = strdup(sqlite_expr->u.zToken);
            break;

        default:
            out->type = EXPR_LITERAL;
            out->value = strdup("UNSUPPORTED_EXPR");
            break;
    }

    return out;
}

MiniExprList* convertExprList( Select* sel) {
    if (!sel || !sel->pEList) return NULL;

    int n = sel->pEList->nExpr;
    MiniExprList* list = (MiniExprList*)malloc(sizeof(MiniExprList));
    list->count = n;
    list->items = (MiniExpr**)malloc(sizeof(MiniExpr*) * n);

    for (int i = 0; i < n; ++i) {
        list->items[i] = convertExpr(sel->pEList->a[i].pExpr);
    }

    return list;
}
