// 文件：sql_stmt.h
#ifndef SQL_STMT_H
#define SQL_STMT_H

#include <stdlib.h>
#include <string.h>

// MiniSQL 语法树结构定义

// SQLStatement 类型枚举
typedef enum {
    STMT_INSERT,
    STMT_SELECT
} MiniStatementType;

// 表达式类型定义
typedef enum {
    EXPR_COLUMN,
    EXPR_LITERAL
} MiniExprType;

// MiniExpr 表达式结构
typedef struct MiniExpr {
    MiniExprType type;
    char* value;  // 对于列名和常量统一用字符串存
} MiniExpr;

// 表达式列表结构
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
} MiniInsertStmt;

// SELECT 语句结构
typedef struct {
    char** column_names;
    int column_count;
    char* table_name;
    MiniExpr* where_expr;
} MiniSelectStmt;

// 顶层 SQLStatement
typedef struct MiniSQLStatement {
    MiniStatementType type;
    union {
        MiniInsertStmt* insert_stmt;
        MiniSelectStmt* select_stmt;
    };
} MiniSQLStatement;

// 解析器 API
MiniSQLStatement* mini_pg_parse_sql(const char* sql);
void free_mini_pg_stmt(MiniSQLStatement* stmt);

// 工具函数
typedef struct {
    int count;
    char** names;
} IdList;

IdList make_idlist(const char* name);
IdList append_idlist(IdList list, const char* name);

// 解析上下文
typedef struct {
    MiniSQLStatement* result;
} ParserContext;

#endif
