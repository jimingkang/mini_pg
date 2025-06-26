#ifndef PARSER_H
#define PARSER_H
#include <stdbool.h>
#include "minidb.h"
#define MAX_COLUMNS 16
#define MAX_COLUMN_NAME_LEN 32
#define MAX_TYPE_LEN 16
#define MAX_TABLE_NAME 32
#define MAX_VALUES 16

#define MAX_WHERE_LEN 128



typedef struct {
    char table_name[MAX_TABLE_NAME];
    int num_columns;
    ColumnDef columns[MAX_COLUMNS];
} CreateTableStmt;

typedef struct {
    char table_name[MAX_TABLE_NAME];
    int num_values;
    const char* values[MAX_VALUES];  // 直接字符串数组
} InsertStmt;

typedef struct {
    char table_name[MAX_TABLE_NAME];
    char columns[MAX_COLUMNS][MAX_COLUMN_NAME_LEN];
    int num_columns;
} SelectStmt;

// 表达式结构，可根据你已有的 SELECT/WHERE 支持扩展
typedef struct {
    char column[MAX_NAME_LEN];  // 列名
    char op[4];                 // 操作符，例如 "="、"!="、"<"
    char value[MAX_WHERE_LEN];  // 值
} Condition;

// Update语句结构
typedef struct {
    char table_name[MAX_NAME_LEN];      // 表名
    int num_assignments;                // 赋值的列数
    char columns[MAX_COLS][MAX_NAME_LEN];  // 要更新的列名
    char* values[MAX_COLS];             // 更新的值
    Condition where;                    // WHERE 子句条件（只支持一个简单条件）
    bool has_where;                     // 是否指定了 WHERE 子句
} UpdateStmt;

bool parse_create_table(const char* sql, CreateTableStmt* stmt);
bool parse_insert(const char* sql, InsertStmt* stmt);
bool parse_select(const char* sql, SelectStmt* stmt);
bool parse_update(const char* sql, UpdateStmt* stmt) ;
#endif