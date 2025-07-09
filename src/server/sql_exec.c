// sql_exec.c
// 封装 SQL 执行接口，供 server 调用

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "server/sql_exec.h"
#include "server/parser.h"     // 假设你的 SQL 解析器定义在这里
#include "server/executor.h"   // 假设实际执行逻辑在这里

bool execute_create_table(sqlite3* sqlite_db, const char* sql,Session session) {
    CreateTableStmt stmt;
    if (!parse_create_table(sql, &stmt)) {
        fprintf(stderr, "[create] parse error\n");
        return false;
    }
     int oid = db_create_table(session.db, stmt.table_name, stmt.columns, stmt.num_columns, session);
    return oid>0;  // 假设返回 0 表示成功
}

bool execute_insert(sqlite3* sqlite_db, const char* sql,Session session) {
   SQLStatement* stmt = mini_pg_parse_sql(sqlite_db, sql);
    InsertStmt* insert = stmt->insert_stmt;

    int idx = find_table(&session.db->catalog, insert->table_name);
    TableMeta *meta = &session.db->catalog.tables[idx];
    if (!meta) return false;


    Tuple user = {0};
    bool flag=convertToTuple(&user,  meta,insert) ;
    if (db_insert(&session.db, "users", &user,session) < 0) {
            fprintf(stderr, "Error: Failed to insert user1\n");
            session_rollback_transaction(&session.db,&session);
            return 1;
        }
        printf("Inserted user1\n");

    return true;
    //return db_insert(db, stmt.table_name, stmt.values);
}

int execute_select_to_string(sqlite3* sqlite_db, const char* sql,Session session,char * ret) {

        printf(" execute_select_to_string snapshot: xmin=%u, xmax=%u, active_xids = [", session.snap.xmin, session.snap.xmax);
for (int i = 0; i < session.snap.active_count; i++) {
    printf("%u,", session.snap.active_xids[i]);
}
printf("]\n");

    SQLStatement* stmt3 = mini_pg_parse_sql(sqlite_db, sql);
    int cnt=0;
      Tuple **new_results = db_query("users",&cnt,session,stmt3->select_stmt);
 char* buf = ret;
memset(buf, 0, 4096);  // 假设 ret 是传入的 buffer，大小为 4KB
size_t offset = 0;

if (new_results) {
    offset += snprintf(buf + offset, 4096 - offset, "Query returned %d tuples:\n", cnt);

    TableMeta* meta = &session.db->catalog.tables[find_table(&(session.db->catalog), "users")];

    for (int i = 0; i < cnt; i++) {
        // 临时格式化每条 tuple
        char line[256] = {0};  // 假设单行最多 256 字节
        format_tuple(line, sizeof(line), new_results[i], meta, session.current_xid);

        // 将 line 拼接到 buf 中
        offset += snprintf(buf + offset, 4096 - offset, "%s", line);
    }
}


    
   return strlen(buf);
   

 
}
int execute_update(sqlite3* sqlite_db, const char* sql, Session session) {
 SQLStatement * stmt = mini_pg_parse_sql(sqlite_db, sql);
    int ret=db_update( stmt->update_stmt,session);
    if (!ret<1) {
        printf("Update failed\n");
       // return;
    } else {
        printf("Update executed successfully\n");
    }
    return ret;
}
