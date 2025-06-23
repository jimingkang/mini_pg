#include "minidb.h"
#include <dirent.h>
#include <fnmatch.h>
#define MAX_COLUMNS 10
// 初始化系统目录
//void init_system_catalog(SystemCatalog *catalog) {
void Old_init_system_catalog(SystemCatalog *catalog, const char *db_path) {
    char path[256];
    snprintf(path, sizeof(path), "%s/catalog.meta", db_path);
   // catalog->table_count = 0;
    //catalog->next_oid = 1000;

     FILE* fp = fopen(path, "rb");
    if (!fp) {
        // 第一次启动，没有 meta 文件，初始化空目录
        catalog->table_count = 0;
        catalog->next_oid = 1000;
        return;
    }

    fread(&catalog->table_count, sizeof(uint16_t), 1, fp);
    fread(&catalog->next_oid, sizeof(uint32_t), 1, fp);

    for (int i = 0; i < catalog->table_count; i++) {
        TableMeta* meta = &catalog->tables[i];
        fread(meta->name, MAX_NAME_LEN, 1, fp);
        fread(meta->filename, MAX_NAME_LEN, 1, fp);
        fread(&meta->oid, sizeof(uint32_t), 1, fp);
        fread(&meta->col_count, sizeof(uint8_t), 1, fp);
        fread(meta->cols, sizeof(ColumnDef), MAX_COLS, fp);
        fread(&meta->first_page, sizeof(PageID), 1, fp);
        fread(&meta->last_page, sizeof(PageID), 1, fp);



        printf("read for table %s, filename=%s\n", meta->name, meta->filename);
    }

    fclose(fp);
}

void init_system_catalog(SystemCatalog *catalog, const char *db_path) {
    catalog->table_count = 0;
    catalog->next_oid = 1000;

    DIR *dir = opendir(db_path);
    if (!dir) {
        perror("Failed to open data directory");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG) continue;
        if (!fnmatch("*.meta", entry->d_name, 0)) {
            if (strcmp(entry->d_name, "catalog.meta") == 0) continue;

            char full_path[256];
            snprintf(full_path, sizeof(full_path), "%s/%s", db_path, entry->d_name);

            FILE *fp = fopen(full_path, "rb");
            if (!fp) continue;

            TableMeta *meta = &catalog->tables[catalog->table_count];
            memset(meta, 0, sizeof(TableMeta));

            // 提取表名（去掉 .meta）
            strncpy(meta->name, entry->d_name, MAX_NAME_LEN);
            char *dot = strstr(meta->name, ".meta");
            if (dot) *dot = '\0';

            snprintf(meta->filename, MAX_NAME_LEN, "%s.tbl", meta->name);

            // 加载字段
            fread(&meta->oid, sizeof(uint32_t), 1, fp);
            fread(&meta->col_count, sizeof(uint8_t), 1, fp);
            fread(meta->cols, sizeof(ColumnDef), MAX_COLS, fp);
            fread(&meta->first_page, sizeof(PageID), 1, fp);
            fread(&meta->last_page, sizeof(PageID), 1, fp);

            fclose(fp);

            catalog->table_count++;
            if (meta->oid >= catalog->next_oid) {
                catalog->next_oid = meta->oid + 1;
            }

             printf("read for table %s, filename=%s,&meta->oid=%d\n", meta->name, meta->filename,meta->oid);
        }
    }

    closedir(dir);
}

// 创建新表

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "catalog.h"

// 保存表元数据到文件
static bool save_table_meta_to_file(const TableMeta *meta, const char *db_path) {
    // 构建元数据文件路径
    char meta_path[MAX_NAME_LEN * 2];
    snprintf(meta_path, sizeof(meta_path), "%s/%s.meta", db_path, meta->name);
    
    FILE *file = fopen(meta_path, "wb");
    if (!file) {
        perror("Failed to create metadata file");
        return false;
    }
    
    // 1. 写入表名
    size_t name_len = strlen(meta->name);
    if (fwrite(&name_len, sizeof(uint8_t), 1, file) != 1) {
        fclose(file);
        return false;
    }
    if (fwrite(meta->name, 1, name_len, file) != name_len) {
        fclose(file);
        return false;
    }
    
    // 2. 写入列数
    if (fwrite(&meta->col_count, sizeof(uint8_t), 1, file) != 1) {
        fclose(file);
        return false;
    }
    
    // 3. 写入每列定义
    for (int i = 0; i < meta->col_count; i++) {
        // 写入列名长度和列名
        size_t col_name_len = strlen(meta->cols[i].name);
        if (fwrite(&col_name_len, sizeof(uint8_t), 1, file) != 1) {
            fclose(file);
            return false;
        }
        if (fwrite(meta->cols[i].name, 1, col_name_len, file) != col_name_len) {
            fclose(file);
            return false;
        }
        
        // 写入列类型
        if (fwrite(&meta->cols[i].type, sizeof(DataType), 1, file) != 1) {
            fclose(file);
            return false;
        }
    }
    
    fclose(file);
    return true;
}

// 创建空的数据文件
static bool create_table_data_file(const TableMeta *meta, const char *db_path) {
    // 构建数据文件路径
    char data_path[MAX_NAME_LEN * 2];
    snprintf(data_path, sizeof(data_path), "%s/%s.tbl", db_path, meta->name);
    
    FILE *file = fopen(data_path, "wb");
    if (!file) {
        perror("Failed to create data file");
        return false;
    }
    
    // 创建一个初始空页面（可选）
    // 在实际实现中，这里可以写入一个空的页面结构
    fclose(file);
    return true;
}

int create_table(SystemCatalog *catalog, const char *table_name, ColumnDef *columns, uint8_t col_count, const char *db_path) {
    if (catalog->table_count >= MAX_TABLES) {
        return -1;
    }
    
    TableMeta *meta = &catalog->tables[catalog->table_count];
    meta->oid = catalog->next_oid++;
    
    // 设置表名（确保不溢出）
    strncpy(meta->name, table_name, MAX_NAME_LEN);
    meta->name[MAX_NAME_LEN - 1] = '\0'; // 确保以null结尾
    
    // 设置数据文件名（使用表名而不是OID）
    snprintf(meta->filename, MAX_NAME_LEN, "%s.tbl", table_name);
    
    meta->col_count = col_count;
    
    // 复制列定义（确保不溢出）
    for (int i = 0; i < col_count; i++) {
        if (i >= MAX_COLUMNS) {
            fprintf(stderr, "Too many columns for table %s\n", table_name);
            return -1;
        }
        
        strncpy(meta->cols[i].name, columns[i].name, MAX_NAME_LEN);
        meta->cols[i].name[MAX_NAME_LEN - 1] = '\0';
        meta->cols[i].type = columns[i].type;
    }
    
    // ===================================================
    // 保存元数据和创建数据文件
    // ===================================================
    if (!save_table_meta_to_file(meta, db_path)) {
        fprintf(stderr, "Failed to save metadata for table %s\n", table_name);
        return -1;
    }
    
    if (!create_table_data_file(meta, db_path)) {
        fprintf(stderr, "Failed to create data file for table %s\n", table_name);
        // 可选：删除已创建的元数据文件
        return -1;
    }
 
    catalog->table_count++;
   //    save_system_catalog(catalog);
    return meta->oid;
}

// 按名称查找表
int find_table(SystemCatalog *catalog, const char *table_name) {
    for (int i = 0; i < catalog->table_count; i++) {
        if (strcmp(catalog->tables[i].name, table_name) == 0) {
            printf("In find_table: found table at %p\n", catalog->tables[i]);

            printf("  oid: %u\n", catalog->tables[i].oid);

            printf("  name: %s\n", catalog->tables[i].name);
          //  return &catalog->tables[i];
          return i;
        }
    }
    return -1;
}

TableMeta* find_table_meta(MiniDB* db, const char* table_name) {
    if (!db || !table_name) return NULL;
    for (int i = 0; i < db->table_count; i++) {
        if (strcmp(db->tables[i].name, table_name) == 0) {
        return &db->tables[i];
        }
    }

    return NULL;

}

void save_system_catalog(const SystemCatalog* catalog) {
    FILE* fp = fopen("catalog.meta", "wb");
    if (!fp) {
        perror("Failed to save system catalog");
        return;
    }

    fwrite(&catalog->table_count, sizeof(uint16_t), 1, fp);
    fwrite(&catalog->next_oid, sizeof(uint32_t), 1, fp);

    for (int i = 0; i < catalog->table_count; i++) {
        const TableMeta* meta = &catalog->tables[i];
        fwrite(meta->name, MAX_NAME_LEN, 1, fp);
        fwrite(meta->filename, MAX_NAME_LEN, 1, fp);
        fwrite(&meta->oid, sizeof(uint32_t), 1, fp);
        fwrite(&meta->col_count, sizeof(uint8_t), 1, fp);
        fwrite(meta->cols, sizeof(ColumnDef), MAX_COLS, fp);
        fwrite(&meta->first_page, sizeof(PageID), 1, fp);
        fwrite(&meta->last_page, sizeof(PageID), 1, fp);
        
        printf("save for table %s, filename=%s\n", meta->name, meta->filename);
    }

    fclose(fp);
}