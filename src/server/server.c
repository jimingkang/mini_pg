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
#define PORT 8888
#define BUFFER_SIZE 4096

MiniDB global_db; // 简化处理，全局数据库对象

void sigchld_handler(int s) {
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

char* handle_query(const char* query, MiniDB* db,Session session ) {
     //fprintf(stderr, "query= %s,strncasecmp(query, select, 6)=%d\n",query,strncasecmp(query, "select", 6));
    // 可根据你项目已有的函数替换这里的调用逻辑
    if (strncasecmp(query, "create table", 12) == 0) {
        if (execute_create_table(db, query,session)) return strdup("Create OK\n");
        else return strdup("Create Failed\n");
    } else if (strncasecmp(query, "insert", 6) == 0) {
        if (execute_insert(db, query,session)) return strdup("Insert OK\n");
        else return strdup("Insert Failed\n");
    } else if (strncasecmp(query, "select", 6) == 0) {
        printf("hit select : query=%s\n",query);
        char* result =malloc(4096);
       int len=  execute_select_to_string(db, query,session,result); // 你需要实现这个函数
        return result ? result : strdup("Select Failed\n");
    } 
    /*
    else if (strncasecmp(query, "update", 6) == 0) {
        // ✅ 新增部分：解析 + 执行 update
         char* result =malloc(4096);
         int len=  execute_update_to_string(db, query,session,result); // 你需要实现这个函数
           printf("update : get retured string from execute_update_to_string:\n%s\n", result);
          return result ? result : strdup("update Failed\n");
    }
          */
    else {
        return strdup("Unsupported SQL\n");
    }
}



int main_pg() {
    signal(SIGCHLD, sigchld_handler);
    
    init_db(&global_db, "/home/rlk/Downloads/mini_pg/build");

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
    if (fork() == 0) {
        close(server_fd);

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
            if (strcmp(buffer, "BEGIN") == 0) {
                session.current_xid= session_begin_transaction(&session);
                printf("BEGIN: %d\n ",session.current_xid);
                const char* msg = "Started transaction\n";
                write(client_fd, msg, strlen(msg));
              //  write(client_fd, "Started transaction\n", 20);
            } else if (strcmp(buffer, "COMMIT") == 0) {
                session_commit_transaction(&global_db,&session);
                const char* msg = "Committed\r\n";
                write(client_fd, msg, strlen(msg));
             //   write(client_fd, "Committed\n", 10);
            }else if (strncasecmp(buffer, "update", 6) == 0) {
                // ✅ 新增部分：解析 + 执行 update
               char* result =malloc(4096);
                int len=  execute_update_to_string(session.db, buffer,session,result); // 你需要实现这个函数
                  printf("update : get retured string from execute_update_to_string:\n%s\n", result);
                return result ? result : strdup("update Failed\n");
         } else {
                // 执行 SQL 时保持 current_xid 状态
                char* result = handle_query(buffer, session.db, session);
                 if (!result) {
                    printf("[handle_query] result is NULL!\n");
                } else {
                    //printf("[handle_query] got result, first char = %c\n", result[0]);
                    printf("handle_query: get retured string from sql_exec:\n%s\n", result);
                }
                write(client_fd, result, strlen(result));
                free(result);
            }
        }

        close(client_fd);
        exit(0);
        }
    close(client_fd);
    }

    close(server_fd);
    return 0;
}
