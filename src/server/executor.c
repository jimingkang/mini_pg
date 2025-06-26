// executor.c
#include "server/executor.h"
#include "tuple.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>




bool db_select(MiniDB* db, const SelectStmt* stmt, ResultSet* result, Session session) {
    int count = 0;
    printf("[debug] select %d columns from table %s\n", stmt->num_columns, stmt->table_name);
    Tuple** tuples = db_query(db, stmt->table_name, &count,session);
    if (!tuples || count == 0) return false;

    //TableMeta* meta = find_table_meta(db, stmt->table_name);
     int idx= find_table(&db->catalog, stmt->table_name);
      printf("[debug] find_table[%d]  \n",idx);
    TableMeta *meta =&(db->catalog.tables[idx]);
    if (!meta) return false;

    result->num_cols = stmt->num_columns;
    result->num_rows = count;
    result->rows = malloc(sizeof(char**) * count);


    printf("[debug]   rows %d from file %s\n", count, meta->filename);
    for (int i = 0; i < count; i++) {
        Tuple* t = tuples[i];
        char** row = malloc(sizeof(char*) * result->num_cols);
        for (int j = 0; j < result->num_cols; j++) {
            const char* colname = stmt->columns[j];

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

            Column* col = &t->columns[col_index];
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
      save_tx_state(&db->tx_mgr, db->data_dir);
    return true;
}

bool old_db_update(MiniDB* db, const UpdateStmt* stmt, Session* session) {

  // if (!db || !stmt || session.current_xid == INVALID_XID) {
  //      fprintf(stderr, "Update failed: Invalid input or no active transaction.\n");
   //     return false;
  //  }

    int idx = find_table(&db->catalog, stmt->table_name);
    if (idx < 0) {
        fprintf(stderr, "Update failed: Table '%s' not found.\n", stmt->table_name);
        return false;
    }

    TableMeta* meta = &db->catalog.tables[idx];
    FILE* fp = fopen(meta->filename, "r+b");
    if (!fp) {
        perror("Failed to open table file");
        return false;
    }

    Page page;
    bool updated = false;
    long pos = 0;

    while (fread(&page, sizeof(Page), 1, fp) == 1) {
        LWLockAcquireExclusive(&page.lock);

        for (int i = 0; i < page.header.slot_count; i++) {
            Slot* slot = &page.slots[i];
            if (slot->status != SLOT_OCCUPIED) continue;

            Tuple* t = page_get_tuple(&page, i,meta); // 假设有个方法能解析 tuple

            // 判断 tuple 是否对当前事务可见
            if (!is_tuple_visible(t, session->current_xid)) continue;

            // 判断是否满足更新条件（匹配 where 条件）
            if (!eval_condition(&(stmt->where), t, meta)) continue;

            // 标记原元组为“被当前事务删除”
            t->xmax = session->current_xid;

            // 构造新 tuple
            Tuple new_tuple = *t; // 复制字段，可替换为 stmt->new_values
            new_tuple.xmin = session->current_xid;
            new_tuple.xmax = INVALID_XID;

            uint16_t new_slot;
            if (!page_insert_tuple(&page, &new_tuple, &new_slot)) {
                fprintf(stderr, "Update failed: No space for new version.\n");
                LWLockRelease(&page.lock);
                fclose(fp);
                return false;
            }

            updated = true;
        }

        // 写回页面
        pos = ftell(fp) - sizeof(Page);
        fseek(fp, pos, SEEK_SET);
        fwrite(&page, sizeof(Page), 1, fp);
        fflush(fp);
        LWLockRelease(&page.lock);
    }

    fclose(fp);
    if (updated) save_table_meta_to_file(meta, db->data_dir);
    return updated;
}



int  db_update(MiniDB *db,const UpdateStmt* stmt,Session session) {
    fprintf(stderr, "stmt->where.column = [%s]\n", stmt->where.column);
    if (!db || !stmt->table_name || session.current_xid == INVALID_XID) {
        fprintf(stderr, "Invalid input or no active transaction\n");
        return false;
    }
    fprintf(stderr, "db_update:table_name '%s'  \n", stmt->table_name);
    int idx = find_table(&db->catalog, stmt->table_name);
    if (idx < 0) {
        fprintf(stderr, "Table '%s' not found\n", stmt->table_name);
        return false;
    }
 //fprintf(stderr, " db_update:table_name '%s' found  \n", stmt->table_name);
    TableMeta *meta = &db->catalog.tables[idx];
    FILE *table_file = fopen(meta->filename, "r+b");
    if (!table_file) {
        perror("Failed to open table file");
        return false;
    }

    int result_count = 0;
    Page page;
    long page_pos = 0;
    while (fread(&page, sizeof(Page), 1, table_file) == 1) {
        //  LWLockAcquireExclusive(&page.lock);
        page_pos = ftell(table_file) - sizeof(Page);  // ✅ 修复定位
        int orig_slot_count = page.header.slot_count;
        fprintf(stderr, "in fread for loop:orig_slot_count=%d,session.current_xid=%d \n", orig_slot_count, session.current_xid);
        for (int i = 0; i < orig_slot_count; i++) {
            //fprintf(stderr, "in fread for loop:orig_slot_count i=%d\n", i);
            Slot *slot = &page.slots[i];
            if (slot->flags  != SLOT_OCCUPIED) continue;

            Tuple *t = page_get_tuple(&page, i, meta);
            if (!t || !is_tuple_visible(t, session.current_xid)) continue;
               // fprintf(stderr, "in fread after is_tuple_visible \n");
            if (t->xmin == session.current_xid) continue;
             fprintf(stderr, "in fread: before  lock_row \n");
           // lock_row(meta->name, t->oid, session.current_xid);
           if (!lock_row(meta->name, t->oid, session.current_xid)) {
    fprintf(stderr, "行锁获取失败，跳过 oid=%u\n", t->oid);
    continue;
}
            
           // if (!eval_condition(&(stmt->where), t, meta)) continue;
           if (!eval_condition(&(stmt->where), t, meta)) {
                fprintf(stderr, "in fread: eval_condition 条件不满足,释放锁 , session.current_xid=%d \n", session.current_xid); 
              unlock_row(meta->name, t->oid, session.current_xid);  // 条件不满足也要释放锁
          
            continue;
            }
                
            sleep(1);   
            t->xmax = session.current_xid;
            Tuple new_t;
            memcpy(&new_t, t, sizeof(Tuple));
            new_t.xmin = session.current_xid;
            new_t.xmax = 0;

            // 遍历所有 SET 列进行更新
            for (int j = 0; j < meta->col_count; j++) {
              for (int k = 0; k < stmt->num_assignments; k++) {
                if (strcmp(meta->cols[j].name, stmt->columns[k]) == 0) {
                   set_column_value(&new_t.columns[j], stmt->values[k]);
                  }
               }
            }

            uint16_t new_slot_idx;
            if (page_insert_tuple(&page, &new_t, &new_slot_idx)) {
                slot->flags = SLOT_DELETED;
               // page.header.tuple_count++;

                (result_count)++;
            }
            fprintf(stderr, "in fread:最终,释放锁 , session.current_x:%d\n", session.current_xid);
            unlock_row(meta->name, t->oid, session.current_xid);
        }

        fseek(table_file, page_pos, SEEK_SET);
        fwrite(&page, sizeof(Page), 1, table_file);
        page_pos = ftell(table_file);
       // LWLockRelease(&page.lock);
    }

    fclose(table_file);
    
  save_tx_state(&db->tx_mgr, db->data_dir);

    for (int i = 0; i < page.header.slot_count; i++) {
    Slot* s = &page.slots[i];
    fprintf(stderr, "Slot[%d] flags=%d offset=%d len=%d\n", i, s->flags, s->offset, s->length);
    
    Tuple* t = page_get_tuple(&page, i, meta);
    if (t) {
        for (int c = 0; c < meta->col_count; c++) {
            if (strcmp(meta->cols[c].name, "name") == 0) {
                fprintf(stderr, "name = %s\n", t->columns[c].value.str_val);
            }
        }
    }
}

    return result_count;
}
void set_column_value(Column* column, const char* new_value) {
    if (!column || !new_value) return;

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
