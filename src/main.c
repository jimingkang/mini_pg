#include "minidb.h"
#include "txmgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <parser.h>
#include "executor.h"
#include "checkpoint.h"
#include "tuple.h"

// 示例程序
int main() {
    MiniDB db;
    // 初始化数据库
    printf("Initializing database...\n");
    init_db(&db, "/home/rlk/Downloads/mini_pg/build");

    sqlite3* sqlite_db = NULL;
    sqlite3_open(":memory:", &sqlite_db);  // 初始化 SQLite 内部状态

   // print_db_status(&db);
    Session session;


    //session.client_fd = client_fd;
    session.db = &db;
    session.current_xid = INVALID_XID;
    /*      
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
        session_rollback_transaction(&db,&session);
        return 1;
    }
   // wal_log_checkpoint(&db);
    
    // 打印中间状态
   // print_db_status(&db);
    
    // 提交事务
    if (session_commit_transaction(session.db,&session) ){
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx1);
        return 1;
    }
    printf("Committed transaction %u\n", tx1);
 */
   
/*
 
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
        
    const char* sql = "insert into users(id,name,age) values(1,'Mesi',20);";
    SQLStatement* stmt = mini_pg_parse_sql(sqlite_db, sql);
    InsertStmt* insert = stmt->insert_stmt;

    int idx = find_table(&session.db->catalog, insert->table_name);
    TableMeta *meta = &session.db->catalog.tables[idx];
    if (!meta) return false;


    Tuple user = {0};
    bool flag=convertToTuple(&user,  meta,insert) ;
    if (db_insert(&db, "users", &user,session) < 0) {
            fprintf(stderr, "Error: Failed to insert user1\n");
            session_rollback_transaction(&db,&session);
            return 1;
        }
        printf("Inserted user1\n");
        
     sql = "insert into users(id,name,age) values(1,'Tom',40) ;";
     
    Tuple user2 = {0};
    stmt = mini_pg_parse_sql(sqlite_db, sql);
        insert = stmt->insert_stmt;
    flag=convertToTuple(&user2,  meta,insert) ;
    if (db_insert(&db, "users", &user2,session) < 0) {
        fprintf(stderr, "Error: Failed to insert user2\n");
        session_rollback_transaction(&db,&session);
        return 1;
    }
    printf("Inserted user2\n");
  // free(user2.columns[1].value.str_val);
//free(user2.columns);
   

    // 打印中间状态
 //   print_db_status(&db);
     
    // 提交事务
    if (session_commit_transaction(&db,&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx2);
        return 1;
    }
    printf("Committed transaction %u\n", tx2);
   */
 
    // ================== 事务 3 ==================
    printf("\n===== Transaction 3: Query Data =====\n");
    
    // 开始新事务
    uint32_t tx3 = session_begin_transaction(&session);
        session.current_xid=tx3;
    if (tx3 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started Query Data transaction %u\n", tx3);

    
    // 查询数据
   // SelectStmt stmt;
     const char* sql2 = "SELECT name FROM users WHERE age > 18 ;";
  //   const char* sql =  "CREATE TABLE users (id INT, name TEXT);";
     SQLStatement *stmt2= mini_pg_parse_sql(sqlite_db, sql2);
    int cnt;
     Tuple**  new_results = db_query("users",&cnt,session,stmt2->select_stmt);
    if (new_results) {

            printf("Query returned %d tuples:\n", cnt);

        for (int i = 0; i < cnt; i++) {
            print_tuple(new_results[i], find_table(&db.catalog, "users"),session.current_xid);
        }
    }

     
    
    
    // 提交事务
    if (session_commit_transaction(&db,&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx3);
        return 1;
    }
    printf("Committed Query transaction %u\n", tx3);
     

  //  wal_log_checkpoint(&db);
 
    // 开始新事务
    uint32_t tx4= session_begin_transaction(&session);
    session.current_xid=tx4;
    // Step 6: 构造并执行 UPDATE 操作
   const char * sql = "update users set age=17 WHERE name = 'Tom' ;";
   SQLStatement * stmt = mini_pg_parse_sql(sqlite_db, sql);
    int ret=db_update( stmt->update_stmt,session);
    if (!ret==1) {
        printf("Update failed\n");
       // return;
    } else {
        printf("Update executed successfully\n");
    }
        // 提交事务
    if (session_commit_transaction(&db,&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx4);
        return 1;
    }
    printf("Committed Updatetransaction %u\n", tx4);

    /**/
    // ================== 事务 5 ==================
    printf("\n===== Transaction 5: Query Data =====\n");
    
    // 开始新事务
    uint32_t tx5 = session_begin_transaction(&session);
        session.current_xid=tx5;
    if (tx3 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx3);
       const char* sql3 = "SELECT name FROM users WHERE age > 18 ;";
 
    SQLStatement* stmt3 = mini_pg_parse_sql(sqlite_db, sql3);
      new_results = db_query("users",&cnt,session,stmt3->select_stmt);
    if (new_results) {

            printf("Query returned %d tuples:\n", cnt);

        for (int i = 0; i < cnt; i++) {
            print_tuple(new_results[i], find_table(&db.catalog, "users"),session.current_xid);
        }
    }

    
    // 提交事务
    if (session_commit_transaction(&db,&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx5);
        return 1;
    }
    printf("Committed 5 transaction %u\n", tx5);
    /*
    // ================== 事务 4 (演示回滚) ==================
    printf("\n===== Transaction 4: Rollback Demo =====\n");
    
    // 开始新事务
    uint32_t tx6 = session_begin_transaction(&session);
    session.current_xid=tx6;
    if (tx4 == INVALID_XID) {
        fprintf(stderr, "Error: Failed to start transaction\n");
        return 1;
    }
    printf("Started transaction %u\n", tx6);
    
    // 插入用户3
    //int col_count=3;
    Tuple user3 = {0};
    user3.col_count = col_count;
    user3.columns = (Column *)malloc(col_count * sizeof(Column));

    user3.columns[0].type = INT4_TYPE; user3.columns[0].value.int_val = 3;
    user3.columns[1].type = TEXT_TYPE; strcpy(user3.columns[1].value.str_val, "Rollback_user");
    user3.columns[2].type = INT4_TYPE; user3.columns[2].value.int_val = 35;
    
    if (db_insert(&db, "users", &user3,session) < 0) {
        fprintf(stderr, "Error: Failed to insert user3\n");
        session_rollback_transaction(&db,&session);
        return 1;
    }
    printf("Inserted user3 (will be rolled back)\n");
    
    // 回滚事务
    if (session_rollback_transaction(&db,&session)) {
        fprintf(stderr, "Error: Failed to rollback transaction %u\n", tx6);
        return 1;
    }
    printf("Rolled back 6 transaction %u\n", tx6);
    
    // ================== 事务 5 ==================
    printf("\n===== Transaction 7: Verify Rollback =====\n");
    
    // 开始新事务
    uint32_t tx7 = session_begin_transaction(&session);
    session.current_xid=tx7;
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

        print_tuple(new_results[i], find_table(&db.catalog, "users"),session.current_xid);

    }
}
        
    // 提交事务
    if (session_commit_transaction(&db,&session)) {
        fprintf(stderr, "Error: Failed to commit transaction %u\n", tx7);
        return 1;
    }
    printf("Committed transaction %u\n", tx7);
*/

    //wal_log_checkpoint(&db);
    // 创建检查点线程
   // db_create_checkpoint(&db);
   //    start_checkpoint_thread(&db);
    
    // 打印最终状态
    printf("\nFinal database status:\n");
    //print_db_status(&db);

    printf("\nDatabase operations completed successfully!\n");
    return 0;
}