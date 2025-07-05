#include "sql_stmt.h"
#include "sqliteInt.h"
#include "parse.h"  // lemon 自动生成
#include <stdio.h>
//#define NULL 0

// 提供封装函数：仅用于解析语法树，不执行 SQL
SQLStatement* mini_pg_parse_sql(sqlite3* db, const char* zSql) {
    Parse sParse;
    memset(&sParse, 0, sizeof(Parse));
    sParse.db = db;

    // 调用 SQLite 的语法解析器
    sqlite3RunParser(&sParse, zSql, 0);

    // 判断是否出错
    if (sParse.rc != SQLITE_OK || sParse.nErr > 0) {
        if (sParse.zErrMsg) {
            fprintf(stderr, "SQL parse error: %s\n", sParse.zErrMsg);
            sqlite3DbFree(db, sParse.zErrMsg);
        } else {
            fprintf(stderr, "Unknown SQL parse error\n");
        }
        return NULL;
    }

    // 由你在 parse.y 中挂载的结构体
    return sParse.pMiniPGStatement;
}
