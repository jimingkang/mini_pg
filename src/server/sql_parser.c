

#include <stdio.h>
#include "sql_stmt.h"
#include "sqliteInt.h"  // 定义了 struct Expr 和 struct Select
#include "types.h"

int convertTkOpToExprOp(int tkOp) {
    switch (tkOp) {
        case TK_EQ: return OP_EQ;
        case TK_GT: return OP_GT;
        case TK_LT: return OP_LT;
        case TK_AND: return OP_AND;
        case TK_OR: return OP_OR;
        default: return -1;  // 不支持的操作
    }
}

MiniExpr* convertExpr(Expr* sqlite_expr) {
    if (!sqlite_expr) return NULL;

    MiniExpr* out = (MiniExpr*)malloc(sizeof(MiniExpr));
    if (!out) return NULL;
    memset(out, 0, sizeof(MiniExpr));  // 初始化内存

    switch (sqlite_expr->op) {
        case TK_STRING:
            // 字符串字面量
            out->type = EXPR_LITERAL;
            out->value = sqlite_expr->u.zToken ? strdup(sqlite_expr->u.zToken) : strdup("");
                printf("[convertExpr] STRING: value = '%s'\n", out->value);
            break;

        case TK_ID:
        case TK_COLUMN:
            // 标识符或列名
            out->type = EXPR_COLUMN;
            out->value = sqlite_expr->u.zToken ? strdup(sqlite_expr->u.zToken) : strdup("<NULL>");
             printf("[convertExpr] COLUMN: name = '%s'\n", out->value);
            break;

        case TK_INTEGER: {
            char buf[32];
            sqlite3_snprintf(sizeof(buf), buf, "%lld", sqlite_expr->u.iValue);
            out->type = EXPR_LITERAL;
            out->value = strdup(buf);
      printf("[convertExpr] INTEGER: value = '%s'\n", out->value);
            break;
        }

        case TK_FLOAT:
            out->type = EXPR_LITERAL;
            out->value = sqlite_expr->u.zToken ? strdup(sqlite_expr->u.zToken) : strdup("0.0");
            printf("[convertExpr] FLOAT: value = '%s'\n", out->value);
            break;

        case TK_GT:
        case TK_LT:
        case TK_EQ:
        case TK_GE:
        case TK_LE:
        case TK_NE:
        case TK_AND:
        case TK_OR:
            out->type = EXPR_BINARY;
            out->op = convertTkOpToExprOp(sqlite_expr->op);
            out->left = convertExpr(sqlite_expr->pLeft);
            out->right = convertExpr(sqlite_expr->pRight);
            printf("[convertExpr] BINARY: op = %d\n", out->op);
            break;

        default:
            // 处理其他类型或错误情况
            fprintf(stderr, "未处理的表达式类型: %d\n", sqlite_expr->op);
            out->type = EXPR_LITERAL;
            out->value = strdup("<UNSUPPORTED>");
            break;
    }

    return out;
}
MiniExpr* old_convertExpr(Expr* sqlite_expr) {
    if (!sqlite_expr) return NULL;

    MiniExpr* out = (MiniExpr*)malloc(sizeof(MiniExpr));
    if (!out) return NULL;

    switch (sqlite_expr->op) {
        case TK_STRING:
        case TK_ID:
        case TK_COLUMN:
            out->type = EXPR_COLUMN;
            out->value = sqlite_expr->u.zToken ? strdup(sqlite_expr->u.zToken) : strdup("<NULL>");
            break;

        case TK_INTEGER: {
            char buf[32];
            sqlite3_snprintf(sizeof(buf), buf, "%lld", sqlite_expr->u.iValue);  // iValue 有效
            out->type = EXPR_LITERAL;
            out->value = strdup(buf);
            break;
        }

        case TK_FLOAT:
            out->type = EXPR_LITERAL;
            out->value = sqlite_expr->u.zToken ? strdup(sqlite_expr->u.zToken) : strdup("<FLOAT>");
            break;

        case TK_GT:
        case TK_LT:
        case TK_EQ:
        case TK_GE:
        case TK_LE:
        case TK_NE:
        case TK_AND:
        case TK_OR:
            out->type = EXPR_BINARY;
            //out->op = sqlite_expr->op;
            out->op = convertTkOpToExprOp(sqlite_expr->op);
            out->left = convertExpr(sqlite_expr->pLeft);
            out->right = convertExpr(sqlite_expr->pRight);
            out->value = NULL;
            break;

        default:
            out->type = EXPR_LITERAL;
            out->value = strdup("<UNSUPPORTED>");
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
MiniExprList* convertExprtoList(Expr* expr) {
    MiniExprList* list = malloc(sizeof(MiniExprList));
    list->count = 1;
    list->items = malloc(sizeof(MiniExpr*));
    list->items[0] = convertExpr(expr);
    return list;
}
