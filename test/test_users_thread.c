#include "minidb.h"
#include <pthread.h>
#include  "executor.h"
const char* shared_table = "users";
void* insert_thread(void* arg) {
    MiniDB* db = (MiniDB*)arg;
    Session session = {.db = db, .client_fd = 0};
    session.current_xid = txmgr_start_transaction(&db->tx_mgr);

    char insert_sql[128];
    snprintf(insert_sql, sizeof(insert_sql), "insert into %s values (1, 'Tom', 20)", shared_table);
    if (!execute_insert(db, insert_sql, session)) {
        printf("[Insert Thread] Insert failed\n");
    } else {
        printf("[Insert Thread] Insert successful\n");
    }

    txmgr_commit_transaction(&db->tx_mgr, session.current_xid);
    return NULL;
}

void* update_thread(void* arg) {
    MiniDB* db = (MiniDB*)arg;
  srand(time(NULL));  // 放在 main() 最开始，只调用一次

    Session session = {.db = db, .client_fd = 0};
    session.current_xid = txmgr_start_transaction(&db->tx_mgr);

    UpdateStmt stmt;
    memset(&stmt, 0, sizeof(UpdateStmt));
    strcpy(stmt.table_name, shared_table);
    stmt.num_assignments = 1;
    strcpy(stmt.columns[0], "age");
   // stmt.values[0] = strdup("99");
   int age = rand() % 101;  // 生成 [0, 100] 范围内的随机数
char age_str[8];
sprintf(age_str, "%d", age);
stmt.values[0] = strdup(age_str);
    strcpy(stmt.where.column, "name");
    strcpy(stmt.where.op, "=");
    strcpy(stmt.where.value, "Jack");

    if (db_update(db, &stmt, session)<=0) {
        printf("[Update Thread] Update failed, age=%d, in thread:%d\n",age ,(unsigned long)pthread_self());
    } else {
        printf("[Update Thread] Update success, age=%d, in thread:%d\n",age ,(unsigned long)pthread_self() );
    }
    int cnt=0;
      Tuple**    new_results = db_query(db, "users",&cnt,session);
    if (new_results) {

            printf("---------new Query returned %d tuples:\n--------", cnt);

        for (int i = 0; i < cnt; i++) {
            print_tuple(new_results[i], find_table(&(db->catalog), "users"));
        }
    }

    txmgr_commit_transaction(&db->tx_mgr, session.current_xid);
    return NULL;
}

int main() {
  
    MiniDB db;
    init_db(&db, "/home/rlk/Downloads/mini_pg/build");

    // 多线程操作
    pthread_t tid_insert, tid_update,tid2_update;
  //  pthread_create(&tid_insert, NULL, insert_thread, &db);
    pthread_create(&tid_update, NULL, update_thread, &db);
       pthread_create(&tid2_update, NULL, update_thread, &db);

   // pthread_join(tid_insert, NULL);
    pthread_join(tid_update, NULL);
    pthread_join(tid2_update, NULL);

    return 0;
}