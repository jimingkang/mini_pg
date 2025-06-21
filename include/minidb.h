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
// 数据库常量
#define PAGE_SIZE 8192
#define HEADER_SIZE 24

#define MAX_COLS 8
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








// 系统目录
typedef struct {
    TableMeta tables[MAX_TABLES];
    uint16_t table_count;
    uint32_t next_oid;       // 下一个对象ID
} SystemCatalog;

// 数据库状态
typedef struct {
    SystemCatalog catalog;   // 系统目录
    TransactionManager tx_mgr; // 事务管理器
    char data_dir[256];      // 数据目录
    uint32_t current_xid;    // 当前活动事务ID
    PageID next_page_id; // 用于分配新页面ID

    TableMeta* tables;      // 表元数据数组

int table_count;        // 表数量
char* db_name;          // 数据库名称
char* db_path;          // 数据库路径
} MiniDB;




// 目录操作函数
//void init_system_catalog(SystemCatalog *catalog);
//int create_table(SystemCatalog *catalog, const char *table_name, ColumnDef *columns, uint8_t col_count);
//TableMeta *find_table(SystemCatalog *catalog, const char *table_name);

// 数据库操作函数
void init_db(MiniDB *db, const char *data_dir);
uint32_t begin_transaction(MiniDB *db);
int commit_transaction(MiniDB *db);
int rollback_transaction(MiniDB *db);
int db_create_table(MiniDB *db, const char *table_name, ColumnDef *columns, uint8_t col_count);

//int db_insert(MiniDB *db, const char *table_name, Tuple *tuple);
bool db_insert(MiniDB *db, const char *table_name,   const Tuple * values);

//int db_query(MiniDB *db, const char *table_name, Tuple *results, int max_results);
Tuple** db_query(MiniDB *db, const char *table_name, int *result_count);
void db_create_checkpoint(MiniDB *db);
void print_db_status(const MiniDB *db);


#endif // MINIDB_H