// 文件：sql_stmt.h
#ifndef SQL_STMT_H
#define SQL_STMT_H

#include <stdlib.h>
#include <string.h>

// SQLStatement 类型枚举
typedef enum {
    STMT_INSERT,
    STMT_SELECT
} StatementType;

// 自定义表达式类型（避免与 SQLite 冲突）
typedef struct MiniExpr {
    enum { EXPR_COLUMN, EXPR_LITERAL } type;
    char* value;  // 对于列名和常量统一用字符串存
} MiniExpr;

typedef struct MiniExprList {
    int count;
    MiniExpr** items;
} MiniExprList;

// INSERT 语句结构
typedef struct {
    char* table_name;
    char** column_names;
    int column_count;
    MiniExprList* values;
} InsertStmt;

// SELECT 语句结构
typedef struct {
    char** column_names;
    int column_count;
    char* table_name;
    MiniExpr* where_expr;
} SelectStmt;

// 顶层 SQLStatement
typedef struct SQLStatement {
    StatementType type;
    union {
        InsertStmt* insert_stmt;
        SelectStmt* select_stmt;
    };
} SQLStatement;
typedef struct Expr Expr;
typedef struct Select Select;
// ---------- sql_parser.c 函数接口 ----------
MiniExpr* convertExpr(Expr* sqlite_expr);
MiniExprList* convertExprList(Select* sel);


#endif