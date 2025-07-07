#include <stdio.h>
#include <sqlite3.h>
#include "sql_stmt.h"
#include "sqliteInt.h"
#include "types.h"
#include "minidb.h"



int main() {
       MiniDB mini_db;
    init_db(&mini_db, "/home/rlk/Downloads/mini_pg/build");
    sqlite3* sqlite_db = NULL;
    sqlite3_open(":memory:", &sqlite_db);  // 初始化 SQLite 内部状态

   const char* sql = "SELECT name FROM users WHERE age > 18 ;";
  //   const char* sql =  "CREATE TABLE users (id INT, name TEXT);";
    SQLStatement* stmt = mini_pg_parse_sql(sqlite_db, sql);

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
        printf("WHERE 条件：");
       // print_expr(sel->where_expr);
        printf("\n");
        } else {
            printf("WHERE 条件：(null)\n");
        }
    Session session = {.db = &mini_db, .client_fd = 0};
    
    // 开始新事务
    uint32_t tx5 = session_begin_transaction(&session);
        session.current_xid=tx5;
    if (tx5 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx5);
  
    int cnt=0;
    Tuple**  new_results = db_query("users",&cnt,session,stmt->select_stmt);
    if (new_results) {

            printf("Query returned %d tuples:\n", cnt);

            int idx = find_table(&session.db->catalog, "users");
            TableMeta *meta = &(session.db->catalog.tables[idx]);

        for (int i = 0; i < cnt; i++) {
            print_tuple(new_results[i], meta,session.current_xid);
        }
    }

    
    // 提交事务
    if (session_commit_transaction(session.db,&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx5);
        return 1;
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

    sqlite3_close(sqlite_db);
    return 0;
}
const char* op_to_string(int op) {
    switch (op) {
        case TK_EQ: return "=";
        case TK_GT: return ">";
        case TK_LT: return "<";
        case TK_GE: return ">=";
        case TK_LE: return "<=";
        case TK_NE: return "!=";
        case TK_AND: return "AND";
        case TK_OR: return "OR";
        default: return "?";
    }
}

void print_expr(MiniExpr* expr) {
    if (!expr) return;

    switch (expr->type) {
        case EXPR_LITERAL:
        case EXPR_COLUMN:
            printf("%s", expr->value);
            break;

        case EXPR_BINARY:
            printf("(");
            print_expr(expr->left);
            printf(" %s ", op_to_string(expr->op));
            print_expr(expr->right);
            printf(")");
            break;

        default:
            printf("<未知表达式>");
            break;
    }
}

