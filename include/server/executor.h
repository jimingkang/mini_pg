// executor.h
#ifndef EXEC_H
#define EXEC_H
#include <stdbool.h>
#include "minidb.h"
#include "tuple.h"


typedef struct {
    int num_rows;
    int num_cols;
    char*** rows; // rows[i][j] 为第 i 行第 j 列数据
} ResultSet;

//bool db_create_table(MiniDB* db, const CreateTableStmt* stmt);
//bool db_insert(MiniDB* db, const char* table_name, const InsertStmt* stmt);
//bool db_select(MiniDB* db, const OldSelectStmt* stmt, ResultSet* result,Session session);
bool db_select(  ResultSet* result, Session session,SelectStmt * stmt);
//bool db_update(MiniDB* db, const UpdateStmt* stmt, Session* session);
//bool exce_update(MiniDB* db, const UpdateStmt* stmt, Session* session);
//int db_update(const OldUpdateStmt* stmt, Session session,SelectStmt * selectStmt);
int db_update(const UpdateStmt* stmt, Session session);
#endif