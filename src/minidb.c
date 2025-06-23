#include "minidb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <zlib.h>
#include "wal.h"
// 初始化数据库
void init_db(MiniDB *db, const char *data_dir) {
    // 设置数据目录
    strncpy(db->data_dir, data_dir, sizeof(db->data_dir));
    //mkdir(data_dir, 0755);
    
    // 初始化系统目录
    init_system_catalog(&db->catalog,db->data_dir);
    
    // 初始化事务管理器
    txmgr_init(&db->tx_mgr);
    
    // 初始无活动事务
    db->current_xid = INVALID_XID;
    
    // 初始化WAL
    init_wal();
    
    // 从WAL恢复
   // recover_from_wal(db);
}



// 开始事务
uint32_t begin_transaction(MiniDB *db) {
    if (db->current_xid != INVALID_XID) {
        fprintf(stderr, "Error: Transaction already in progress\n");
        return INVALID_XID;
    }
    
    db->current_xid = txmgr_start_transaction(&db->tx_mgr);
    return db->current_xid;
}

// 提交事务
int commit_transaction(MiniDB *db) {
    if (db->current_xid == INVALID_XID) {
        fprintf(stderr, "Error: No active transaction\n");
        return -1;
    }
    
    txmgr_commit_transaction(&db->tx_mgr, db->current_xid);
    wal_log_commit(db->current_xid);
    db->current_xid = INVALID_XID;
    return 0;
}

// 回滚事务
int rollback_transaction(MiniDB *db) {
    if (db->current_xid == INVALID_XID) {
        fprintf(stderr, "Error: No active transaction\n");
        return -1;
    }
    
    txmgr_abort_transaction(&db->tx_mgr, db->current_xid);
    wal_log_abort(db->current_xid);
    db->current_xid = INVALID_XID;
    return 0;
}

uint32_t session_begin_transaction(Session* session) {
    if (session->current_xid != INVALID_XID) {
        fprintf(stderr, "Error: transaction already started\n");
        return INVALID_XID;
    }

    session->current_xid = txmgr_start_transaction(&session->db->tx_mgr);
    return session->current_xid;
}

int session_commit_transaction(Session* session) {
    if (session->current_xid == INVALID_XID) {
        fprintf(stderr, "Error: no active transaction\n");
        return -1;
    }

    txmgr_commit_transaction(&session->db->tx_mgr, session->current_xid);
    wal_log_commit(session->current_xid);
    session->current_xid = INVALID_XID;
    return 0;
}
int session_rollback_transaction(Session* session) {
    if (!session || session->current_xid == INVALID_XID) {
        fprintf(stderr, "[session] No active transaction to rollback\n");
        return -1;
    }

    txmgr_abort_transaction(&session->db->tx_mgr, session->current_xid);
    session->current_xid = INVALID_XID;
    printf("[session] Rolled back transaction\n");
    return 0;
}


// 创建表
int db_create_table(MiniDB *db, const char *table_name, ColumnDef *columns, uint8_t col_count,Session session) {
    //db->current_xid=xid;
    if (session.current_xid == INVALID_XID) {
        fprintf(stderr, "Error: No active transaction in db_create_table\n");
        return -1;
    }
    
    // 在系统目录中创建表
    int oid = create_table(&db->catalog, table_name, columns, col_count,"./");
    if (oid < 0) {
        fprintf(stderr, "Error: Failed to create table '%s'\n", table_name);
        return -1;
    }
    
    // 获取表元数据
    int idx= find_table(&db->catalog, table_name);
    TableMeta *users_meta =&(db->catalog.tables[idx]); //很奇怪,如果find_table返回return &catalog->tables[i];会有问题
   if (users_meta) {
        printf("Found table 'users':\n");
        printf("  OID: %u\n", users_meta->oid);
        printf("  Filename: %s\n", users_meta->filename);
        printf("  Columns: %d\n", users_meta->col_count);
        for (int i = 0; i < users_meta->col_count; i++) {
            printf("    %s (%d)\n", 
                   users_meta->cols[i].name, 
                   users_meta->cols[i].type);
        }
    } else {
        printf("Table 'users' not found\n");
    }

    // 记录WAL
    wal_log_create_table(users_meta, db->current_xid);
    
    return oid;
}

/**
 * 向表中插入新元组
 * 
 * @param db 数据库实例
 * @param table_name 表名
 * @param values 列值数组
 * @return 是否成功
 */
bool db_insert(MiniDB *db, const char *table_name,   const Tuple * values,Session session) {
    if (!db || !table_name || !values) {
        return false;
    }
        if (session.current_xid == INVALID_XID) {
        fprintf(stderr, "Error: No active transaction in db_create_table\n");
        return -1;
    }
    
    // 查找表元数据
    //TableMeta *meta = find_table_meta(db, table_name);
    int idx= find_table(&db->catalog, table_name);
    TableMeta *meta =&(db->catalog.tables[idx]); 
    if (!meta) {
        fprintf(stderr, "Table '%s' not found\n", table_name);
        return false;
    }
    
    // 创建新元组
    Tuple* new_tuple =values;// create_tuple(meta, values);
    if (!new_tuple) {
        fprintf(stderr, "Failed to create tuple\n");
        return false;
    }
    
    // 分配新OID
    static uint32_t next_oid = 1;
    new_tuple->oid = next_oid++;
    
    // 打开表文件
    FILE *table_file = fopen(meta->filename, "r+b");
    if (!table_file) {
        perror("Failed to open table file");
        free_tuple(new_tuple);
        return false;
    }
    
    // 查找有空间的页面
    Page page;
    bool found_space = false;
    long insert_pos = 0;
    uint16_t slot_index;
    
    while (fread(&page, sizeof(Page), 1, table_file) == 1) {
        // 检查空闲空间
        size_t required_space = new_tuple->col_count * sizeof(Column) + 128; // 估算大小
        
        if (page_free_space(&page) >= required_space) {
            // 尝试插入
            if (page_insert_tuple(&page, new_tuple, &slot_index)) {
                found_space = true;
                insert_pos = ftell(table_file) - sizeof(Page);
                break;
            }
        }
    }
    
    // 如果没有空间，创建新页面
    if (!found_space) {
        PageID new_page_id = db->next_page_id++;
        page_init(&page, new_page_id);
        
        if (!page_insert_tuple(&page, new_tuple, &slot_index)) {
            fprintf(stderr, "Failed to insert into new page\n");
            fclose(table_file);
            free_tuple(new_tuple);
            return false;
        }
        
        // 移动到文件末尾
        fseek(table_file, 0, SEEK_END);
        insert_pos = ftell(table_file);
    }
    
    // 写入页面
    fseek(table_file, insert_pos, SEEK_SET);
    if (fwrite(&page, sizeof(Page), 1, table_file) != 1) {
        perror("Failed to write page");
        fclose(table_file);
        free_tuple(new_tuple);
        return false;
    }
    
    fclose(table_file);
   // free_tuple(new_tuple);
    return true;
}
/**
 * 查询表中的所有元组
 * 
 * @param db 数据库实例
 * @param table_name 表名
 * @param result_count 返回结果数量
 * @return 元组指针数组，需要调用者释放
 */
Tuple** db_query(MiniDB *db, const char *table_name, int *result_count) {
    if (!db || !table_name || !result_count) {
        return NULL;
    }
    
    *result_count = 0;
    
    // 查找表元数据
  int idx= find_table(&db->catalog, table_name);
    TableMeta *meta =&(db->catalog.tables[idx]);
    if (!meta) {
        fprintf(stderr, "Table '%s' not found\n", table_name);
        return NULL;
    }
    fprintf(stderr, "for Table '%s',file  found:%s\n", table_name,meta->filename);
    
    // 打开表文件
    FILE *table_file = fopen(meta->filename, "rb");
    if (!table_file) {
        perror("Failed to open table file");
        return NULL;
    }
    
    // 分配结果数组
    Tuple** results = malloc(MAX_RESULTS * sizeof(Tuple*));
    if (!results) {
        fclose(table_file);
        return NULL;
    }
    
    // 读取页面
    Page page;
    int page_count = 0;
    int total_tuples = 0;
    
    while (fread(&page, sizeof(Page), 1, table_file) == 1) {
        page_count++;
        
        // 验证页面
        if (page.header.page_id == INVALID_PAGE_ID) {
            continue;
        }
        
        // 获取槽位数组
        Slot* slots = (Slot*)page.data;
        
        // 遍历所有槽位
        for (int i = 0; i < page.header.slot_count; i++) {
            if (!(slots[i].flags & SLOT_OCCUPIED)) {
                continue; // 跳过未占用槽位
            }
            
            // 反序列化元组
            uint8_t* tuple_data = page.data + slots[i].offset;
            Tuple* tuple = (Tuple*)malloc(sizeof(Tuple));
            if (!tuple) {
                continue;
            }
            
            if (deserialize_tuple(tuple, tuple_data) != slots[i].length) {
                free(tuple);
                continue;
            }
            
            // 添加到结果集
            if (total_tuples < MAX_RESULTS) {
                results[total_tuples++] = tuple;
            } else {
                free_tuple(tuple); // 结果集已满
            }
        }
    }
    
    fclose(table_file);
    
    // 处理结果
    if (total_tuples == 0) {
        free(results);
        results = NULL;
    } else {
        // 调整结果数组大小
        Tuple** tmp = realloc(results, total_tuples * sizeof(Tuple*));
        if (tmp) {
            results = tmp;
        }
    }
    
    *result_count = total_tuples;
    return results;
}


// 释放查询结果
void free_query_results(Tuple** results, int count) {
    if (!results) return;
    
    for (int i = 0; i < count; i++) {
        if (results[i]) {
            free_tuple(results[i]);
        }
    }
    free(results);
}
// 创建检查点
void db_create_checkpoint(MiniDB *db) {
    wal_log_checkpoint();
}

// 打印数据库状态
void print_db_status(const MiniDB *db) {
    printf("\n===== Database Status =====\n");
    printf("Data Directory: %s\n", db->data_dir);
    printf("Current Transaction ID: %u\n", db->current_xid);
    
    // 打印系统目录状态
    printf("\nSystem Catalog:\n");
    printf("  Tables: %d\n", db->catalog.table_count);
    for (int i = 0; i < db->catalog.table_count; i++) {
        const TableMeta *meta = &db->catalog.tables[i];
        printf("  - %s (OID: %u, File: %s)\n", 
               meta->name, meta->oid, meta->filename);
        printf("    Columns: %d\n", meta->col_count);
        for (int j = 0; j < meta->col_count; j++) {
            printf("      %s: %s\n", 
                   meta->cols[j].name,
                   meta->cols[j].type == INT4_TYPE ? "INT" : "TEXT");
        }
    }
    
    // 打印事务管理器状态
    txmgr_print_status(&db->tx_mgr);
}





