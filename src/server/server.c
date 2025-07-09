// server.c
// 基于 mini_pg 的 TCP 服务端，用于接收客户端 SQL 请求并返回执行结果

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include "minidb.h"  // 需要你已有的 mini_pg 接口头文件
#include "server/parser.h"     // 假设你的 SQL 解析器定义在这里
#include "server/executor.h"   // 假设实际执行逻辑在这里
#include "server/sql_exec.h"   // 假设实际执行逻辑在这里

#include <sys/ipc.h>
#include <sys/shm.h>
#include <pthread.h>
#include "xid_info.h"

//XidInfo* xid_info = NULL;  // 定义全局指针

//int xid_shmid = -1;        // 保存共享内存 id，方便释放

#define PORT 8888
#define BUFFER_SIZE 4096

MiniDB global_db; // 简化处理，全局数据库对象

 sqlite3* sqlite_db = NULL;


void sigchld_handler(int s) {
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

char* handle_sql(const char* query, sqlite3* sqlite_db,Session session ) {
     session.current_xid= tcp_session_begin_transaction(&session);
     //fprintf(stderr, "query= %s,strncasecmp(query, select, 6)=%d\n",query,strncasecmp(query, "select", 6));
    // 可根据你项目已有的函数替换这里的调用逻辑
    if (strncasecmp(query, "create table", 12) == 0) {
        if (execute_create_table(sqlite_db, query,session)) 
        { tcp_session_commit_transaction(&global_db,&session);
            return strdup("Create OK\n");
        }
        else 
        {
            tcp_session_rollback_transaction(&global_db,&session);
           return strdup("Create Failed\n");
        }
     
    }
     else if (strncasecmp(query, "insert", 6) == 0) {
        if (execute_insert(sqlite_db, query,session))
         { tcp_session_commit_transaction(&global_db,&session);
             return strdup("Insert OK\n");
         }
        else  { 
            tcp_session_rollback_transaction(&global_db,&session);
             return strdup("Insert Failed\n");
        }
    }
    else if (strncasecmp(query, "update", 6) == 0) {
                // ✅ 新增部分：解析 + 执行 update
                int len=  execute_update(sqlite_db, query,session); // 你需要实现这个函数
                 tcp_session_commit_transaction(&global_db,&session);
                return len>0 ? strdup("update sucessfully\n") : strdup("update Failed\n");
         } 
    
    else if (strncasecmp(query, "select", 6) == 0) {
        printf("hit select : query=%s\n",query);
        char* result =malloc(4096);
       int len=  execute_select_to_string(sqlite_db, query,session,result); // 你需要实现这个函数
         tcp_session_commit_transaction(&global_db,&session);
        return result ? result : strdup("Select Failed\n");
    } 
    else {
        return strdup("Unsupported SQL\n");
    }
}

void handle_exit(MiniDB *global_db, int signo) {
    if (xid_info) {
        tcp_save_tx_state(global_db->tx_mgr, global_db->db_path, xid_info->next_xid);
    }

    // 可选：释放共享内存
    if (xid_shmid != -1) {
        shmctl(xid_shmid, IPC_RMID, NULL);  // 删除共享内存段
    }

    printf("Server exiting gracefully.\n");
    exit(0);
}


void* client_thread(void* arg) {
    int client_fd = *(int*)arg;
    free(arg);
        Session session;
        session.client_fd = client_fd;
        session.db = &global_db;
        session.current_xid = INVALID_XID;
        char buffer[4096];
        while (1) {
            memset(buffer, 0, sizeof(buffer));
            int n = read(client_fd, buffer, sizeof(buffer));
        
            if (n <= 0) {
            printf("[server] read() returned %d, client disconnected?\n", n);
            break;
            }
             buffer[strcspn(buffer, "\n")] = 0;
            printf("[client %d] received (%d bytes): %s\n", client_fd, n, buffer);
            if (strcmp(buffer, "START") == 0) {
                session.current_xid= tcp_session_begin_transaction(&session);
                const char* msg =malloc(256);
                memset(msg,0,256);
                sprintf(msg,"thread:%x,BEGIN: %d\n ",(unsigned long)getpid(),session.current_xid);
                printf("thread:%x,BEGIN: %d\n ",(unsigned long)getpid(),session.current_xid);
                write(client_fd, msg, strlen(msg));
              //  write(client_fd, "Started transaction\n", 20);
            } else if (strcmp(buffer, "COMMIT") == 0) {
                tcp_session_commit_transaction(&global_db,&session);
                const char* msg =malloc(256);
                  memset(msg,0,256);
                 sprintf(msg,"thread:%x,Committed: %d\r\n ",(unsigned long)getpid(),session.current_xid);
                  printf("thread:%x,Committed: %d\r\n ",(unsigned long)getpid(),session.current_xid);
                write(client_fd, msg, strlen(msg));
             //   write(client_fd, "Committed\n", 10);
            } else {
                // 执行 SQL 时保持 current_xid 状态
                char* result = handle_sql(buffer, sqlite_db, session);
                 if (!result) {
                    printf("[handle_sql] result is NULL!\n");
                } else {
                    printf("thread:%x,%s\n",(unsigned long)getpid(), result);
                }
                write(client_fd, result, strlen(result));
                free(result);
            }
        }

      
    close(client_fd);
    return NULL;
}

int main() {
    signal(SIGCHLD, sigchld_handler);
    signal(SIGINT, handle_exit);
    signal(SIGTERM, handle_exit);
   
    sqlite3_open(":memory:", &sqlite_db);  // 初始化 SQLite 内部状态
    
    tcp_init_db(&global_db, "/home/rlk/Downloads/mini_pg/build");
 

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 10);

    printf("[mini_pg] server started on port %d\n", PORT);

   while (1) {
    int client_fd = accept(server_fd, NULL, NULL);
    int* pfd = malloc(sizeof(int));
    *pfd = client_fd;
    pthread_t tid;
    pthread_create(&tid, NULL, client_thread, pfd);
    pthread_detach(tid); // 自动回收资源
    }

    close(server_fd);
    return 0;
}

