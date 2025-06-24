// executor.c
#include "server/executor.h"
#include "tuple.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>


bool demo_db_select(MiniDB* db, const SelectStmt* stmt, ResultSet* result) {
    // 此处为模拟，实际应调用你的 select 执行逻辑
    result->num_rows = 1;
    result->num_cols = stmt->num_columns;
    result->rows = malloc(sizeof(char**) * result->num_rows);
    result->rows[0] = malloc(sizeof(char*) * result->num_cols);

    for (int i = 0; i < result->num_cols; i++) {
        result->rows[0][i] = strdup("demo");
    }
    return true;
}
/*
//直接tuple读取数据
bool direct_tuple_db_select(MiniDB* db, const SelectStmt* stmt, ResultSet* result) {
  //  TableMeta* meta = find_table(&db->catalog, stmt->table_name);
    int idx= find_table(&db->catalog, stmt->table_name);
    TableMeta *meta =&(db->catalog.tables[idx]);
    if (!meta) {
        fprintf(stderr, "[select] table not found: %s\n", stmt->table_name);
        return false;
    }

    FILE* table_file = fopen(meta->filename, "rb");
    if (!table_file) {
        perror("[select] failed to open table file");
        return false;
    }

    Tuple tuple;
    result->num_cols = stmt->num_columns;
    result->rows = NULL;
    result->num_rows = 0;

    while (read_tuple(table_file, &tuple)) {
        if (tuple.deleted) continue;

        // 构造一行字符串结果
        char** row = malloc(sizeof(char*) * result->num_cols);
        for (int i = 0; i < result->num_cols; i++) {
            const char* colname = stmt->columns[i];

            int col_index = -1;
            for (int j = 0; j < meta->col_count; j++) {
                if (strcmp(meta->cols[j].name, colname) == 0) {
                    col_index = j;
                    break;
                }
            }
            if (col_index == -1) {
                fprintf(stderr, "[select] column not found: %s\n", colname);
                fclose(table_file);
                return false;
            }

            Column* col = &tuple.columns[col_index];
            char buf[128];
            switch (col->type) {
                case INT4_TYPE:
                    snprintf(buf, sizeof(buf), "%d", col->value.int_val);
                    break;
                case FLOAT_TYPE:
                    snprintf(buf, sizeof(buf), "%.2f", col->value.float_val);
                    break;
                case BOOL_TYPE:
                    snprintf(buf, sizeof(buf), "%s", col->value.bool_val ? "true" : "false");
                    break;
                case TEXT_TYPE:
                    snprintf(buf, sizeof(buf), "%s", col->value.str_val);
                    break;
                case DATE_TYPE:
                    snprintf(buf, sizeof(buf), "%d", col->value.int_val);
                    break;
                default:
                    strcpy(buf, "<unknown>");
            }
            row[i] = strdup(buf);
        }

        result->rows = realloc(result->rows, sizeof(char**) * (result->num_rows + 1));
        result->rows[result->num_rows++] = row;

        if (tuple.columns) {
            for (int i = 0; i < tuple.col_count; i++) {
                if (tuple.columns[i].type == TEXT_TYPE && tuple.columns[i].value.str_val)
                    free(tuple.columns[i].value.str_val);
            }
            free(tuple.columns);
        }
    }

    fclose(table_file);
    return true;
}
*/
//通过page读取数据
bool page_db_select(MiniDB* db, const SelectStmt* stmt, ResultSet* result) {
    TableMeta* meta = find_table_meta(&db->catalog, stmt->table_name);
    if (!meta) {
        fprintf(stderr, "[select] table not found: %s\n", stmt->table_name);
        return false;
    }

    FILE* table_file = fopen(meta->filename, "rb");
    if (!table_file) {
        perror("[select] open file failed");
        return false;
    }

    result->num_cols = stmt->num_columns;
    result->num_rows = 0;
    result->rows = NULL;

    PageID current = meta->first_page;
    while (current != INVALID_PAGE_ID) {
        Page* page = read_page(table_file, current);
        if (!page) break;

        for (int i = 0; i < page->header.tuple_count; i++) {
            Tuple* tuple = page_get_tuple(page, i,meta);
            if (!tuple || tuple->deleted) continue;

            // 构造一行
            char** row = malloc(sizeof(char*) * result->num_cols);
            for (int c = 0; c < result->num_cols; c++) {
                const char* colname = stmt->columns[c];
                int col_index = -1;
                for (int j = 0; j < meta->col_count; j++) {
                    if (strcmp(meta->cols[j].name, colname) == 0) {
                        col_index = j;
                        break;
                    }
                }
                if (col_index == -1) {
                    fprintf(stderr, "[select] column not found: %s\n", colname);
                    fclose(table_file);
                    return false;
                }

                Column* col = &tuple->columns[col_index];
                char buf[128];
                switch (col->type) {
                    case INT4_TYPE:
                        snprintf(buf, sizeof(buf), "%d", col->value.int_val);
                        break;
                    case FLOAT_TYPE:
                        snprintf(buf, sizeof(buf), "%.2f", col->value.float_val);
                        break;
                    case BOOL_TYPE:
                        snprintf(buf, sizeof(buf), "%s", col->value.bool_val ? "true" : "false");
                        break;
                    case TEXT_TYPE:
                        snprintf(buf, sizeof(buf), "%s", col->value.str_val);
                        break;
                    case DATE_TYPE:
                        snprintf(buf, sizeof(buf), "%d", col->value.int_val);
                        break;
                    default:
                        strcpy(buf, "<unknown>");
                }

                row[c] = strdup(buf);
            }

            result->rows = realloc(result->rows, sizeof(char**) * (result->num_rows + 1));
            result->rows[result->num_rows++] = row;

            free_tuple(tuple);  // 释放动态字段
        }

        PageID next = page->header.next_page;
        free_page(page);  // 你自己的释放函数
        current = next;
    }

    fclose(table_file);
    return true;
}


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
    return true;
}

