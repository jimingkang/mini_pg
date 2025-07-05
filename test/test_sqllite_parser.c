#include <stdio.h>
#include <sqlite3.h>
#include "sql_stmt.h"
#include "sqliteInt.h"

// 声明解析函数（你应在 .h 文件里添加）：
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
            fprintf(stderr, "Unknown SQL parse error\n");
        }
        return NULL;
    }

    // 由你在 parse.y 中挂载的结构体
    return sParse.pMiniPGStatement;
}

int main() {
    sqlite3* db = NULL;
    sqlite3_open(":memory:", &db);  // 初始化 SQLite 内部状态

    const char* sql = "SELECT name FROM users WHERE age > 18;";
    SQLStatement* stmt = mini_pg_parse_sql(db, sql);

    if (!stmt) {
        printf("解析失败。\n");
        return 1;
    }

    if (stmt->type == STMT_SELECT) {
        SelectStmt* sel = stmt->select_stmt;
        printf("SELECT 语句，表名：%s\n", sel->table_name);
        printf("字段：");
        for (int i = 0; i < sel->column_count; ++i) {
            printf("%s ", sel->column_names[i]);
        }
        printf("\n");
        if (sel->where_expr) {
            printf("WHERE 条件：%s\n", sel->where_expr->value);
        }
    } else if (stmt->type == STMT_INSERT) {
        InsertStmt* ins = stmt->insert_stmt;
        printf("INSERT 语句，表名：%s\n", ins->table_name);
        for (int i = 0; i < ins->column_count; ++i) {
            printf("列：%s = %s\n",
                ins->column_names[i],
                ins->values->items[i]->value);
        }
    }

    sqlite3_close(db);
    return 0;
}
