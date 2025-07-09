// executor.c
#include "server/executor.h"
#include "tuple.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>




bool db_select(  ResultSet* result, Session session,SelectStmt * stmt) {
    int count = 0;
    printf("[debug] select %d columns from table %s\n", stmt->where_expr->count, stmt->table_name);
    Tuple** tuples = db_query( stmt->table_name, &count,session,stmt);
    if (!tuples || count == 0) return false;

    //TableMeta* meta = find_table_meta(db, stmt->table_name);
     int idx= find_table(&session.db->catalog, stmt->table_name);
      printf("[debug] find_table[%d]  \n",idx);
    TableMeta *meta =&(session.db->catalog.tables[idx]);
    if (!meta) return false;

    result->num_cols = stmt->column_count;
    result->num_rows = count;
    result->rows = malloc(sizeof(char**) * count);


    printf("[debug]   rows %d from file %s\n", count, meta->filename);
    for (int i = 0; i < count; i++) {
        Tuple* t = tuples[i];
        char** row = malloc(sizeof(char*) * result->num_cols);
        for (int j = 0; j < result->num_cols; j++) {
            const char* colname = stmt->column_names[j];

            // 查找列索引
            int col_index = -1;
            for (int k = 0; k < meta->col_count; k++) {
                if (strcmp(meta->cols[k].name, colname) == 0) {
                    col_index = k;
                    break;
                }
            }
            if (col_index == -1) {
                row[j] = strdup("<invalid>");
                continue;
            }

            OldColumn* col = &t->columns[col_index];
            char buf[128];
            switch (col->type) {
                case INT4_TYPE: snprintf(buf, sizeof(buf), "%d", col->value.int_val); break;
                case FLOAT_TYPE: snprintf(buf, sizeof(buf), "%.2f", col->value.float_val); break;
                case BOOL_TYPE: snprintf(buf, sizeof(buf), "%s", col->value.bool_val ? "true" : "false"); break;
                case TEXT_TYPE: snprintf(buf, sizeof(buf), "%s", col->value.str_val); break;
                case DATE_TYPE: snprintf(buf, sizeof(buf), "%d", col->value.int_val); break;
                default: strcpy(buf, "<unknown>"); break;
            }

            row[j] = strdup(buf);
       
        }
           
        result->rows[i] = row;
        free_tuple(t);  // ✅释放每条 Tuple
    }

    free(tuples);
      save_tx_state(session.db->tx_mgr, session.db->data_dir);
    return true;
}

int db_update(const UpdateStmt* stmt, Session session) {
    MiniDB *db=session.db;
    if (!db || !stmt->table_name || session.current_xid == INVALID_XID) {
        fprintf(stderr, "Invalid input or no active transaction\n");
        return false;
    }

    int idx = find_table(&db->catalog, stmt->table_name);
    if (idx < 0) {
        fprintf(stderr, "Table '%s' not found\n", stmt->table_name);
        return false;
    }



    TableMeta *meta = &db->catalog.tables[idx];

    char fullpath[256];
    snprintf(fullpath, sizeof(fullpath), "%s/%s", db->data_dir, meta->filename);
    strcpy(meta->fillpath,fullpath);
    int result_count = 0;

    //for (PageID page_id = 0; page_id < db->next_page_id; page_id++) {
    for (PageID page_id = meta->first_page; page_id <= meta->last_page; page_id++) {

        Page *page = page_cache_load_or_fetch(page_id, meta);
        // LWLockAcquireExclusive(&page->lock);
        if (!page) continue;

        int orig_slot_count = page->header.slot_count;

        for (int i = 0; i < orig_slot_count; i++) {
            Slot *slot = &page->slots[i];
            if (slot->flags != SLOT_OCCUPIED) continue;

            Tuple *t = page_get_tuple(page, i, meta);

            printf("[visible] xid=%u checks tuple {xmin=%u, xmax=%u}, committed(xmin)=%d, session.snap.xmin=%u\n", session.current_xid, t->xmin, t->xmax,
            txmgr_is_committed(db->tx_mgr, t->xmin),session.snap.xmin);
            if (!t || !is_tuple_visible(meta,db->tx_mgr,t, session.current_xid,&session.snap)) {
                free_tuple(t);
                continue;
            }

            if (t->xmin == session.current_xid) {
                free_tuple(t);
                continue; // 不重复更新本事务插入的行
            }

         

            if (!eval_condition(stmt->where_expr, t, meta)) {

                free_tuple(t);
                continue;
            }
            if (!lock_row(meta->name, t->oid, session.current_xid)) {
                fprintf(stderr, "xid:%d,行锁获取失败，跳过 oid=%u\n",session.current_xid, t->oid);
                free_tuple(t);
                continue;
            }
            sleep(1);  // 模拟并发延迟

            // 逻辑删除旧元组
            t->xmax = session.current_xid;

           // page_update_tuple(page, i, t); // 更新 xmax
            if (!page_update_tuple(page, i, t)) {
                fprintf(stderr, "xid=%d: Failed to update old tuple xmax\n", session.current_xid);
                unlock_row(meta->name, t->oid, session.current_xid);
                free_tuple(t);
                continue;
            }
        

            // 插入新版本元组
            Tuple new_t;
            memcpy(&new_t, t, sizeof(Tuple));
                //free_tuple(t);
            new_t.xmin = session.current_xid;
            new_t.xmax = 0;
            new_t.deleted=false;


            for (int j = 0; j < meta->col_count; j++) {
                for (int k = 0; k < stmt->column_count; k++) {
                    if (strcmp(meta->cols[j].name, stmt->column_names[k]) == 0) {
                       // printf("[DEBUG] stmt->values[%d] = '%s'\n",k, stmt->values[k] ? stmt->values[k] : "NULL");
                       // set_column_value(&new_t.columns[j], stmt->values[k]);
                       MiniExpr* expr = stmt->values[k];
                        if (expr && expr->type == EXPR_LITERAL && expr->value) {
                            set_column_value_from_expr(&new_t.columns[j], expr);
                        } else {
                            fprintf(stderr, "暂不支持复杂表达式或空值更新：列 %s\n", stmt->column_names[k]);
                        }
                    }
                }
            }

            uint16_t new_slot_idx;
            if (page_insert_tuple(page, &new_t, &new_slot_idx)) {
                result_count++;
            }

            unlock_row(meta->name, new_t.oid, session.current_xid);
        }
        page_cache_mark_dirty(page_id);
        // 修改后的页需刷回磁盘
       page_cache_flush(page_id, fullpath);
        //LWLockRelease(&page->lock);
    }

   // save_tx_state(db->tx_mgr, db->data_dir);
    return result_count;
}

void set_column_value(OldColumn* column, const char* new_value) {

   // if (!column || !new_value) return;
   if (!column || !new_value || strlen(new_value) == 0) {
        fprintf(stderr, "[set_column_value] 警告：参数非法或空字符串 new_value='%s'\n",
                new_value ? new_value : "NULL");
        return;
    }
    switch (column->type) {
        case INT4_TYPE:
            column->value.int_val = atoi(new_value);
            break;
        case TEXT_TYPE:
            strncpy(column->value.str_val, new_value, MAX_TEXT_LEN - 1);
            column->value.str_val[MAX_TEXT_LEN - 1] = '\0';  // 保证结尾
            break;
        default:
            fprintf(stderr, "Unsupported column type in set_column_value\n");
            break;
    }
}

void set_column_value_from_expr(OldColumn* column, MiniExpr* expr) {
    if (!column || !expr || expr->type != EXPR_LITERAL || !expr->value) {
        fprintf(stderr, "[set_column_value] 非法表达式，必须是字面量\n");
        return;
    }

    switch (column->type) {
        case INT4_TYPE:
            column->value.int_val = atoi(expr->value);
            break;
        case TEXT_TYPE:
            if (!column->value.str_val) {
                column->value.str_val = malloc(MAX_TEXT_LEN);
            }
            strncpy(column->value.str_val, expr->value, MAX_TEXT_LEN - 1);
            column->value.str_val[MAX_TEXT_LEN - 1] = '\0';
            break;
        default:
            fprintf(stderr, "Unsupported column type in set_column_value\n");
            break;
    }
}

