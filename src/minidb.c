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
#include "lock.h"
#include "executor.h"
#include "txmgr.h"


const char *DATADIR=NULL;
// 初始化数据库
void init_db(MiniDB *db, const char *data_dir) {
    // 设置数据目录
    strncpy(db->data_dir, data_dir, sizeof(db->data_dir));
    mkdir(data_dir, 0755);
    DATADIR=data_dir;
    
    // 初始化系统目录
    init_system_catalog(&db->catalog,db->data_dir);
    
    // 初始化事务管理器
    txmgr_init(&db->tx_mgr);
    load_tx_state(&(db->tx_mgr), data_dir);
    // 初始无活动事务
    db->current_xid = INVALID_XID;
    db->next_page_id = 0;  // 如果是新数据库
    
    init_page_cache();
    init_row_lock_table();
    // 初始化WAL
    init_wal();
    
    // 从WAL恢复
   // recover_from_wal(db);
}


/*
// 开始事务
uint32_t begin_transaction(MiniDB *db) {
    if (db->current_xid != INVALID_XID) {
        fprintf(stderr, "Error: Transaction already in progress\n");
        return INVALID_XID;
    }
    
    db->current_xid = txmgr_start_transaction(db);
    return db->current_xid;
}

// 提交事务
int commit_transaction(MiniDB *db) {
    if (db->current_xid == INVALID_XID) {
        fprintf(stderr, "Error: No active transaction\n");
        return -1;
    }
    
    txmgr_commit_transaction(db, db->current_xid);
    wal_log_commit(db->current_xid);
    db->current_xid = INVALID_XID;

   // save_tx_state(&db->tx_mgr, db->data_dir);
    return 0;
}

// 回滚事务
int rollback_transaction(MiniDB *db) {
    if (db->current_xid == INVALID_XID) {
        fprintf(stderr, "Error: No active transaction\n");
        return -1;
    }
    
    txmgr_abort_transaction(db, db->current_xid);
    wal_log_abort(db->current_xid);
    db->current_xid = INVALID_XID;
   // save_tx_state(&db->tx_mgr, db->data_dir);
    return 0;
}
*/
uint32_t session_begin_transaction(Session* session) {
    if (session->current_xid != INVALID_XID) {
        fprintf(stderr, "Error: transaction already started\n");
        return INVALID_XID;
    }

    session->current_xid = txmgr_start_transaction(session->db);
   // session->snapshot_xmin = session->db->tx_mgr.next_xid - 1;  // 只看到 <= 这个XID的提交数据
   // session->snapshot_xmin =c(&session->db->tx_mgr);
   compute_snapshot(&session->db->tx_mgr,session->current_xid,&(session->snap));
    return session->current_xid;
}

int session_commit_transaction(MiniDB *db,Session* session) {
    if (session->current_xid == INVALID_XID) {
        fprintf(stderr, "Error: no active transaction\n");
        return -1;
    }

    txmgr_commit_transaction(db, session->current_xid);
    wal_log_commit(session->current_xid);
    //unlock_all_rows_for_xid(session->current_xid); // ✅ 显式释放所有行锁
    session->current_xid = INVALID_XID;

    //save_tx_state(&db->tx_mgr, db->data_dir);
    return 0;
}


int session_rollback_transaction(MiniDB *db, Session* session) {
    if (!session || session->current_xid == INVALID_XID) {
        fprintf(stderr, "[session] No active transaction to rollback\n");
        return -1;
    }

    uint32_t xid = session->current_xid;

    for (int i = 0; i < db->catalog.table_count; i++) {
        TableMeta* meta = &db->catalog.tables[i];
        char fullpath[256];
        snprintf(fullpath, sizeof(fullpath), "%s/%s", db->data_dir, meta->filename);

        for (PageID page_id = 0; page_id < db->next_page_id; page_id++) {
            Page* page = page_cache_load_or_fetch(page_id, meta);
            if (!page) continue;

            int modified = 0;
            LWLockAcquireExclusive(&page->lock);
            for (int j = 0; j < page->header.slot_count; j++) {
                Tuple* tuple = page_get_tuple(page, j, meta);
                if (!tuple) continue;

                if (tuple->xmin == xid) {
                    page_delete_tuple(page, j);
                    modified = 1;
                    printf("[rollback] Removed tuple with oid=%u from table '%s'\n",
                           tuple->oid, meta->name);
                }

                free_tuple(tuple);
            }
            LWLockRelease(&page->lock);

            if (modified) {
                page_cache_mark_dirty(page_id);
                page_cache_flush(page_id, fullpath);
            }
        }
    }

    txmgr_abort_transaction(db, xid);
    session->current_xid = INVALID_XID;
   // save_tx_state(&db->tx_mgr, db->data_dir);
    printf("[session] Rolled back transaction %u\n", xid);
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
    int oid = create_table(&db->catalog, table_name, columns, col_count,&db->data_dir);
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
Tuple** db_query(const char *table_name, int *result_count, Session session,SelectStmt* selectStmt) {
    MiniDB *db=session.db;
    if (!db || !table_name || !result_count) return NULL;

    *result_count = 0;
    int idx = find_table(&db->catalog, table_name);
    if (idx < 0) return NULL;

    TableMeta *meta = &db->catalog.tables[idx];
    if (!meta) return NULL;

    char fullpath[256];
    snprintf(fullpath, sizeof(fullpath), "%s/%s", db->data_dir, meta->filename);
    strcpy(meta->fillpath,fullpath);
    Tuple** results = malloc(MAX_RESULTS * sizeof(Tuple*));
    if (!results) return NULL;

    int total_tuples = 0;

    // 用于记录已读的 offset，防止重复（最多支持一页最多 MAX_SLOTS 个 slot）
    bool seen_offsets[PAGE_SIZE] = { false };

    for (PageID page_id = meta->first_page; page_id <= meta->last_page; page_id++) {
        Page* page = page_cache_load_or_fetch(page_id, meta);
        if (!page || page->header.page_id == INVALID_PAGE_ID) continue;

        Slot* slots = page->slots;
        for (int i = 0; i < page->header.slot_count; i++) {
            Slot* slot = &slots[i];

            // 跳过无效/删除的slot
            if (slot->flags & SLOT_DELETED || slot->flags == 0) continue;

            // 防止重复读取同一 offset 上的 tuple
            if (slot->offset < PAGE_SIZE && seen_offsets[slot->offset]) {
                continue; // 跳过重复 tuple
            }
            seen_offsets[slot->offset] = true;

            Tuple* t = page_get_tuple(page, i, meta);
            if (!t) continue;

            // MVCC 可见性判断
            bool visible = is_tuple_visible(meta,&db->tx_mgr, t, session.current_xid,&(session.snap));
            if (visible) {
                if (total_tuples < MAX_RESULTS) {
                    if (selectStmt && selectStmt->where_expr) {
                        if (!eval_expr(selectStmt->where_expr, t, meta)) {
                            free_tuple(t);
                            continue;
                        }
                    }

                 
                    results[total_tuples++] = t;
                } else {
                    free_tuple(t);
                }
            } else {
                free_tuple(t);
            }
            
        }
    }

    if (total_tuples == 0) {
        free(results);
        return NULL;
    } else {
        Tuple** tmp = realloc(results, total_tuples * sizeof(Tuple*));
        if (tmp) results = tmp;
    }
    *result_count = total_tuples;

    for (int i = 0; i < total_tuples; i++) {
            print_tuple(results[i], meta,session.current_xid);
        }
    return results;
}


bool db_insert(MiniDB *db, const char *table_name, const Tuple *values, Session session) {
    if (!db || !table_name || !values || session.current_xid == INVALID_XID) return false;

    int idx = find_table(&db->catalog, table_name);
    TableMeta *meta = &db->catalog.tables[idx];
    if (!meta) return false;

    Tuple *new_tuple = (Tuple *)values;
    new_tuple->oid = ++meta->max_row_oid;
    new_tuple->xmin = session.current_xid;

    char fullpath[256];
    snprintf(fullpath, sizeof(fullpath), "%s/%s", db->data_dir, meta->filename);
strcpy(meta->fillpath,fullpath);
    FILE* fp = fopen(fullpath, "r+b");
    if (!fp) {
        fp = fopen(fullpath, "w+b");
    }
    fseek(fp, 0, SEEK_END);
    if (ftell(fp) == 0) {
        Page empty;
        page_init(&empty, 0);
        fwrite(&empty, sizeof(Page), 1, fp);
    }
    fclose(fp);

    LWLockAcquireExclusive(&meta->fsm_lock);
    Page *page = NULL;
    PageID page_id;
    bool inserted = false;

    for (page_id = meta->first_page; page_id <= meta->last_page; page_id++) {
        page = page_cache_load_or_fetch(page_id, meta);
        if (!page) continue;

        size_t required_space = new_tuple->col_count * sizeof(Column) + 128;
        if (page_free_space(page) >= required_space) {
            LWLockAcquireExclusive(&page->lock);
            uint16_t slot_index;
            if (page_insert_tuple(page, new_tuple, &slot_index)) {
                page_cache_mark_dirty(page_id);
                inserted = true;
                LWLockRelease(&page->lock);
                break;
            }
            LWLockRelease(&page->lock);
        }
    }
    LWLockRelease(&meta->fsm_lock);

    if (!inserted) {
        LWLockAcquireExclusive(&meta->extension_lock);
        PageID new_page_id = ++meta->last_page;

        Page *new_page = page_cache_load_or_fetch(new_page_id, meta);
        if (!new_page) {
            Page temp_page;
            page_init(&temp_page, new_page_id);
            LWLockInit(&temp_page.lock, 0);
            //global_page_cache.entries[new_page_id % PAGE_CACHE_SIZE].table_oid=temp_page.
            global_page_cache.entries[new_page_id % PAGE_CACHE_SIZE].page = temp_page;
            global_page_cache.entries[new_page_id % PAGE_CACHE_SIZE].page_id = new_page_id;
            global_page_cache.entries[new_page_id % PAGE_CACHE_SIZE].valid = true;
            global_page_cache.entries[new_page_id % PAGE_CACHE_SIZE].dirty = true;
            new_page = &global_page_cache.entries[new_page_id % PAGE_CACHE_SIZE].page;
        }

        LWLockAcquireExclusive(&new_page->lock);
        uint16_t slot_index;
        if (!page_insert_tuple(new_page, new_tuple, &slot_index)) {
            LWLockRelease(&new_page->lock);
            LWLockRelease(&meta->extension_lock);
            return false;
        }
        page_cache_mark_dirty(new_page_id);
        LWLockRelease(&new_page->lock);
        LWLockRelease(&meta->extension_lock);

        page_id = new_page_id; // 修正关键点：确保最后flush的是新页
    }

    page_cache_flush(page_id, fullpath);
    return true;
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
    wal_log_checkpoint(db);
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


// 提供封装函数：仅用于解析语法树，不执行 SQL
SQLStatement* mini_pg_parse_sql(sqlite3* db, const char* zSql) {
    Parse sParse;
    memset(&sParse, 0, sizeof(Parse));
    sParse.db = db;

    // 调用 SQLite 的语法解析器
    sqlite3RunParser(&sParse, zSql);

    // 判断是否出错
    if (sParse.rc != SQLITE_OK || sParse.nErr > 0) {
        if (sParse.zErrMsg) {
            fprintf(stderr, "SQL parse error: %s\n", sParse.zErrMsg);
            sqlite3DbFree(db, sParse.zErrMsg);
        } else {
           // fprintf(stderr, "Unknown SQL parse \n");
        }
      //  return NULL;
    }

    // 由你在 parse.y 中挂载的结构体
    return sParse.pMiniPGStatement;
}
