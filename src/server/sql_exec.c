// sql_exec.c
// 封装 SQL 执行接口，供 server 调用

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "server/sql_exec.h"
#include "server/parser.h"     // 假设你的 SQL 解析器定义在这里
#include "server/executor.h"   // 假设实际执行逻辑在这里

bool execute_create_table(MiniDB* db, const char* sql,Session session) {
    CreateTableStmt stmt;
    if (!parse_create_table(sql, &stmt)) {
        fprintf(stderr, "[create] parse error\n");
        return false;
    }
     int ok = db_create_table(db, stmt.table_name, stmt.columns, stmt.num_columns, session);
    return ok == 0;  // 假设返回 0 表示成功
}

bool execute_insert(MiniDB* db, const char* sql,Session session) {
    InsertStmt stmt;
    if (!parse_insert(sql, &stmt)) {
        fprintf(stderr, "[insert] parse error\n");
        return false;
    }
    // 获取表结构
    int idx= find_table(&db->catalog, stmt.table_name);
     TableMeta *meta =&(db->catalog.tables[idx]); 
    if (!meta) {
        fprintf(stderr, "Table '%s' not found\n", stmt.table_name);
        return false;
    }
   

   Tuple tuple;
    tuple.oid = 0;  // 可用 static 变量分配唯一 OID
    tuple.xmin =session.current_xid;
    tuple.xmax = 0;
    tuple.deleted = false;
    tuple.col_count = meta->col_count;
    tuple.columns = malloc(sizeof(Column) * tuple.col_count);
    if (!tuple.columns) return false;

    for (int i = 0; i < tuple.col_count; i++) {
        Column* col = &tuple.columns[i];
        const char* raw = stmt.values[i];
        col->type = meta->cols[i].type;

        switch (col->type) {
            case INT4_TYPE:
                col->value.int_val = atoi(raw);
                break;
            case FLOAT_TYPE:
                col->value.float_val = strtof(raw, NULL);
                break;
            case BOOL_TYPE:
                col->value.bool_val = (strcmp(raw, "true") == 0 || strcmp(raw, "1") == 0);
                break;
            case TEXT_TYPE:
                col->value.str_val = strdup(raw);
                break;
            case DATE_TYPE:
                col->value.int_val = atoi(raw);  // 暂存为整数
                break;
            default:
                fprintf(stderr, "[insert] unsupported column type\n");
                free(tuple.columns);
                return false;
        }
    }

    bool ok = db_insert(db, stmt.table_name, &tuple, session);

    for (int i = 0; i < tuple.col_count; i++) {
        if (tuple.columns[i].type == TEXT_TYPE) {
            free(tuple.columns[i].value.str_val);
        }
    }
    free(tuple.columns);

    return ok;
    //return db_insert(db, stmt.table_name, stmt.values);
}

int execute_select_to_string(MiniDB* db, const char* sql,Session session,char * ret) {
    SelectStmt stmt;
    if (!parse_select(sql, &stmt)) {
        fprintf(stderr, "[select] parse error\n");
        return NULL;
    }

    ResultSet result;
    if (!db_select(db, &stmt, &result,session)) {
        fprintf(stderr, "[select] execution failed\n");
        return NULL;
    }

    /*
    char* buf = malloc(4096); // 简易实现，建议动态扩容
    buf[0] = '\0';
    for (int i = 0; i < result.num_rows; i++) {
        for (int j = 0; j < result.num_cols; j++) {
            strcat(buf, result.rows[i][j]);
            strcat(buf, "\t");
        }
        strcat(buf, "\n");
    }
        */
    char* buf = ret;//malloc(4096);
    memset(buf,0,4096);
    if (!buf) return NULL;
    size_t offset = 0;
    for (int i = 0; i < result.num_rows; i++) {
        for (int j = 0; j < result.num_cols; j++) {
            const char* val = result.rows[i][j] ? result.rows[i][j] : "<null>";
            offset += snprintf(buf + offset, 4096 - offset, "%s\t", val);
        }
        offset += snprintf(buf + offset, 4096 - offset, "\n");
    }
   // strcat(buf, "\0");
  //printf("[select]  execute_select_to_string:len=%d\n, %s",strlen(buf),buf);
   //  return buf;
   return strlen(buf);
   
 // char* debug = strdup(buf);  // 新申请一份，避免指针混乱
//printf("[select] strdup done: debug=%s\n", debug);
//return debug;
 
}
