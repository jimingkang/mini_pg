#ifndef MINIDB_H
#define MINIDB_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>

#include "txmgr.h"
#include "page.h"
#include "types.h"
#include "parser.h"'

// 数据库常量
//#define PAGE_SIZE 8192
#define HEADER_SIZE 24

//#define MAX_COLS 8
#define MAX_NAME_LEN 32
#define MAX_STRING_LEN 256
#define SYSTEM_CATALOG "pg_class.dat"
#define MAX_RESULTS 1000

/*

// 页面头部结构
typedef struct {
    uint32_t checksum;
    uint32_t lsn;
    uint16_t lower;
    uint16_t upper;
    uint16_t special;
    uint16_t flags;
} PageHeader;
*/












// 数据库操作函数
void init_db(MiniDB *db, const char *data_dir);

uint32_t begin_transaction(MiniDB *db);
int commit_transaction(MiniDB *db);
int rollback_transaction(MiniDB *db);

int session_commit_transaction(MiniDB *db,Session* session) ;
uint32_t session_begin_transaction(Session* session); 
int session_rollback_transaction(MiniDB *db,Session* session);

int db_create_table(MiniDB *db, const char *table_name, ColumnDef *columns, uint8_t col_count,Session session);

//int db_insert(MiniDB *db, const char *table_name, Tuple *tuple);
bool db_insert(MiniDB *db, const char *table_name,   const Tuple * values,Session session);
//bool db_update(MiniDB* db, const UpdateStmt* stmt, Session session);
//bool db_update(MiniDB *db, const char *table_name,const UpdateStmt* stmt, int *result_count, Session session);

//int db_query(MiniDB *db, const char *table_name, Tuple *results, int max_results);
Tuple** db_query(MiniDB *db, const char *table_name, int *result_count,Session session);
void db_create_checkpoint(MiniDB *db);
void print_db_status(const MiniDB *db);

#endif // MINIDB_H