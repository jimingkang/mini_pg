// 文件：sql_stmt.h
#ifndef SQL_STMT_H
#define SQL_STMT_H

#include <stdlib.h>
#include <string.h>

// SQLStatement 类型枚举
typedef enum {
    STMT_INSERT,
    STMT_SELECT,
    STMT_UPDATE
} StatementType;

typedef enum {
    EXPR_COLUMN,
    EXPR_LITERAL,
    EXPR_BINARY   // 新增的类型
} MiniExprType;

typedef struct MiniExpr {
    MiniExprType type;

    // 对于 EXPR_LITERAL / EXPR_COLUMN
    char* value;

    // 对于 EXPR_BINARY
    int op;                    // 如 TK_GT, TK_LT 等
    struct MiniExpr* left;     // 左子树
    struct MiniExpr* right;    // 右子树
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
    MiniExprList* where_expr;
} SelectStmt;
typedef struct {
    char* table_name;             // 表名
    char** column_names;          // 要更新的列名数组
    //** values;                // 与 column_names 对应的新值（字符串形式）
    MiniExpr**values;           // 与 column_names 对应的新值 (MiniExpr**)
    int column_count;             // 列数
    MiniExprList* where_expr;         // 可选 WHERE 表达式
} UpdateStmt;
// 顶层 SQLStatement
typedef struct SQLStatement {
    StatementType type;
    union {
        InsertStmt* insert_stmt;
        SelectStmt* select_stmt;
        UpdateStmt* update_stmt;
    };
} SQLStatement;
typedef struct Expr Expr;
typedef struct Select Select;
typedef struct Insert Insert;
// ---------- sql_parser.c 函数接口 ----------
MiniExpr* convertExpr(Expr* sqlite_expr);
MiniExprList* convertExprList(Select* sel);
MiniExprList* convertExprtoList(Expr* expr);


#endif