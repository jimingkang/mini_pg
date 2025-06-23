// executor.h
#ifndef EXEC_H
#define EXEC_H
#include <stdbool.h>
#include "minidb.h"
#include "parser.h"

typedef struct {
    int num_rows;
    int num_cols;
    char*** rows; // rows[i][j] 为第 i 行第 j 列数据
} ResultSet;

//bool db_create_table(MiniDB* db, const CreateTableStmt* stmt);
//bool db_insert(MiniDB* db, const char* table_name, const InsertStmt* stmt);
bool db_select(MiniDB* db, const SelectStmt* stmt, ResultSet* result,Session session);
#endif