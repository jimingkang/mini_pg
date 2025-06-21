#ifndef CATALOG_H
#define CATALOG_H

#include <stdint.h>

#define MAX_NAME_LEN 64
#define MAX_COLS 32
#define MAX_TABLES 128

#include "minidb.h"
#include "tuple.h"
// 数据库目录（表目录）
typedef struct {
    TableMeta tables[MAX_TABLES];    // 表数组
    int table_count;                 // 当前表数量
    uint32_t next_oid;
} Catalog;



// 函数声明
void init_catalog(Catalog *catalog);
//int create_table(Catalog *catalog, const char *table_name, ColumnDef *columns, uint8_t col_count);
//int create_table(SystemCatalog *catalog, const char *table_name, ColumnDef *columns, uint8_t col_count);
int create_table(SystemCatalog *catalog, const char *table_name, ColumnDef *columns, uint8_t col_count, const char *db_path);
//TableMeta* find_table(Catalog *catalog, const char *table_name);
int find_table(SystemCatalog *catalog, const char *table_name);

TableMeta* find_table_by_oid(SystemCatalog *catalog, uint32_t oid);
int update_table_meta(SystemCatalog *catalog, TableMeta *meta); // 添加缺失的声明

TableMeta* find_table_meta(MiniDB* db, const char* table_name);

#endif // CATALOG_H