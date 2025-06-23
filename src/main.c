#include "minidb.h"
#include "txmgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


// 示例程序
int main() {
    MiniDB db;
    
    // 初始化数据库
    printf("Initializing database...\n");
    init_db(&db, "/home/rlk/Downloads/mini_pg/build/bin/");
    print_db_status(&db);
    Session session;


    //session.client_fd = client_fd;
    session.db = &db;
    session.current_xid = INVALID_XID;
    /*    */
    // ================== 事务 1 ==================
    printf("\n===== Transaction 1: Create Table =====\n");
    
    // 开始事务
    uint32_t tx1 =session_begin_transaction(&session);
    session.current_xid =tx1;
    if (tx1 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx1);
    
    // 创建表定义
    ColumnDef user_columns[] = {
        {"id", INT4_TYPE},
        {"name", TEXT_TYPE},
        {"age", INT4_TYPE}
    };
    
    // 创建表
    if (db_create_table(&db, "users", user_columns, 3,session) < 0) {
        fprintf(stderr, "Error: Failed to create table\n");
        session_rollback_transaction(&db);
        return 1;
    }
    
    // 打印中间状态
    print_db_status(&db);
    
    // 提交事务
    if (session_commit_transaction(&session) ){
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx1);
        return 1;
    }
    printf("Committed transaction %u\n", tx1);

    // ================== 事务 2 ==================
    printf("\n===== Transaction 2: Insert Data =====\n");
    
    // 开始新事务
    uint32_t tx2 = session_begin_transaction(&session);
        session.current_xid =tx2;
    if (tx2 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx2);
    
    // 插入用户1
    Tuple user1 = {0};
  

    uint8_t col_count = 3;
user1.col_count = col_count;
user1.columns = (Column *)malloc(col_count * sizeof(Column));


    user1.columns[0].type = INT4_TYPE; user1.columns[0].value.int_val = 1;
    user1.columns[1].type = TEXT_TYPE; user1.columns[1].value.str_val = strdup("Mesi");
    user1.columns[2].type = INT4_TYPE; user1.columns[2].value.int_val = 30;

    if (db_insert(&db, "users", &user1,    session) < 0) {
        fprintf(stderr, "Error: Failed to insert user1\n");
        session_rollback_transaction(&session);
        return 1;
    }
    printf("Inserted user1\n");
    free(user1.columns[1].value.str_val);
free(user1.columns);
    
    /*   */
    // 插入用户2
    Tuple user2 = {0};
    user2.col_count = col_count;
    user2.columns = (Column *)malloc(col_count * sizeof(Column));
    user2.columns[0].type = INT4_TYPE; user2.columns[0].value.int_val = 2;
    user2.columns[1].type = TEXT_TYPE; strcpy(user2.columns[1].value.str_val, "Kaka");
    user2.columns[2].type = INT4_TYPE; user2.columns[2].value.int_val = 25;
    
    if (db_insert(&db, "users", &user2,session) < 0) {
        fprintf(stderr, "Error: Failed to insert user2\n");
        session_rollback_transaction(&session);
        return 1;
    }
    printf("Inserted user2\n");
  // free(user2.columns[1].value.str_val);
//free(user2.columns);
   

    // 打印中间状态
    print_db_status(&db);
    
    // 提交事务
    if (session_commit_transaction(&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx2);
        return 1;
    }
    printf("Committed transaction %u\n", tx2);
    
    // ================== 事务 3 ==================
    printf("\n===== Transaction 3: Query Data =====\n");
    
    // 开始新事务
    uint32_t tx3 = session_begin_transaction(&session);
        session.current_xid=tx3;
    if (tx3 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx3);
    
    // 查询数据
    int cnt;
     Tuple**  new_results = db_query(&db, "users",&cnt,session);
    if (new_results) {

            printf("Query returned %d tuples:\n", cnt);

        for (int i = 0; i < cnt; i++) {
            print_tuple(new_results[i], find_table(&db.catalog, "users"));
        }
    }
    
    // 提交事务
    if (session_commit_transaction(&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx3);
        return 1;
    }
    printf("Committed transaction %u\n", tx3);
    
    // ================== 事务 4 (演示回滚) ==================
    printf("\n===== Transaction 4: Rollback Demo =====\n");
    
    // 开始新事务
    uint32_t tx4 = session_begin_transaction(&session);
    session.current_xid=tx4;
    if (tx4 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx4);
    
    // 插入用户3
    Tuple user3 = {0};
        user3.col_count = col_count;
    user3.columns = (Column *)malloc(col_count * sizeof(Column));

    user3.columns[0].type = INT4_TYPE; user3.columns[0].value.int_val = 3;
    user3.columns[1].type = TEXT_TYPE; strcpy(user3.columns[1].value.str_val, "Rollback_user");
    user3.columns[2].type = INT4_TYPE; user3.columns[2].value.int_val = 35;
    
    if (db_insert(&db, "users", &user3,session) < 0) {
        fprintf(stderr, "Error: Failed to insert user3\n");
        session_rollback_transaction(&session);
        return 1;
    }
    printf("Inserted user3 (will be rolled back)\n");
    
    // 回滚事务
    if (session_rollback_transaction(&session)) {
        fprintf(stderr, "Error: Failed to rollback transaction %u\n", tx4);
        return 1;
    }
    printf("Rolled back transaction %u\n", tx4);
    
    // ================== 事务 5 ==================
    printf("\n===== Transaction 5: Verify Rollback =====\n");
    
    // 开始新事务
    uint32_t tx5 = session_begin_transaction(&session);
    session.current_xid=tx5;
    if (tx5 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx5);
    
    // 查询数据（用户3应该不存在）
    //count = db_query(&db, "users", results, &count);
    int res_count;
    new_results = db_query(&db, "users", &res_count,session);
    if (new_results) {

        printf("Query returned %d tuples:\n", res_count);

        for (int i = 0; i < res_count; i++) {

        print_tuple(new_results[i], find_table(&db.catalog, "users"));

    }
}
        
    // 提交事务
    if (session_commit_transaction(&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx5);
        return 1;
    }
    printf("Committed transaction %u\n", tx5);
    
    // 创建检查点
    db_create_checkpoint(&db);
    
    // 打印最终状态
    printf("\nFinal database status:\n");
    print_db_status(&db);
    
    printf("\nDatabase operations completed successfully!\n");
    return 0;
}