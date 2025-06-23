#ifndef PARSER_H
#define PARSER_H
#include <stdbool.h>
#include "minidb.h"
#define MAX_COLUMNS 16
#define MAX_COLUMN_NAME_LEN 32
#define MAX_TYPE_LEN 16
#define MAX_TABLE_NAME 32
#define MAX_VALUES 16



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

bool parse_create_table(const char* sql, CreateTableStmt* stmt);
bool parse_insert(const char* sql, InsertStmt* stmt);
bool parse_select(const char* sql, SelectStmt* stmt);
#endif