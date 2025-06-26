#include "minidb.h"
#include "parser.h"
#include <assert.h>
#include <executor.h>
#include <sql_exec.h>
void test_update_case(MiniDB* db) {
    // Step 1: 启动事务
    Session session;
    session.db = db;
    session.client_fd = 0;
    session.current_xid = txmgr_start_transaction(&( session.db ->tx_mgr));

    
    // Step 2: 创建表
    const char* create_sql = "create table users (id INT, name TEXT, age INT)";
    if (!execute_create_table(db, create_sql, session)==0) {
        printf("Create table failed\n");
        return;
    }

    // Step 3: 插入初始数据
    const char* insert_sql = "insert into users values (1, 'Tom', 20)";
    if (!execute_insert(db, insert_sql, session)==1) {
        printf("Insert failed\n");
        return;
    }
    

    // Step 4: 构造并执行 UPDATE 操作
    UpdateStmt stmt;
    memset(&stmt, 0, sizeof(UpdateStmt));
    strcpy(stmt.table_name, "users");

    stmt.num_assignments = 1;
     strcpy(stmt.columns[0], "age");
    //strcpy(stmt.values[0] ,"40");
    stmt.values[0]=strdup("40");


    
    strcpy(stmt.where.column, "name");
    strcpy(stmt.where.value, "Tom");

    if (!db_update(db,  &stmt, session)==1) {
        printf("Update failed\n");
       // return;
    } else {
        printf("Update executed successfully\n");
    }
    txmgr_commit_transaction(&( session.db ->tx_mgr), session.current_xid );

    // Step 5: 查询并验证
    SelectStmt sel;
    memset(&sel, 0, sizeof(SelectStmt));
    strcpy(sel.table_name, "users");
    sel.num_columns = 3;
    strcpy(sel.columns[0], "id");
    strcpy(sel.columns[1], "name");
    strcpy(sel.columns[2], "age");

    ResultSet result;
    memset(&result, 0, sizeof(ResultSet));
    if (!db_select(db, &sel, &result, session)) {
        printf("Select failed\n");
        return;
    }

    printf("Query Result:\n");
    for (int i = 0; i < result.num_rows; ++i) {
        for (int j = 0; j < result.num_cols; ++j) {
            printf("%s\t", result.rows[i][j]);
        }
        printf("\n");
    }
}
int main() {
    MiniDB db;
    init_db(&db, "/home/rlk/Downloads/mini_pg/build/bin"); // 自己已有的数据目录初始化逻辑
   // init_tx_manager(&db);       // 初始化事务管理器

    test_update_case(&db);

    return 0;
}
