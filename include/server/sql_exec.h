#ifndef SQL_EXEC_H
#define SQL_EXEC_H
#include <stdbool.h>
#include "minidb.h"

// 封装接口：供 server.c 调用的统一 SQL 执行入口
bool execute_create_table(sqlite3* sqlite_db, const char* sql,Session session);
bool execute_insert(sqlite3* sqlite_db, const char* sql,Session session );
//char* execute_select_to_string(MiniDB* db, const char* sql,Session session) ;

int execute_update_to_string(sqlite3* sqlite_db, const char* sql, Session session, char* output); 
int execute_select_to_string(sqlite3* sqlite_db, const char* sql,Session session,char * ret);
#endif
