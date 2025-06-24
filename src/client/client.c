// client.c
// 与 mini_pg 服务端交互，支持多轮 SQL 输入

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8888
#define BUFFER_SIZE 8192

int main() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        return 1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connection failed");
        close(sockfd);
        return 1;
    }

    char query[BUFFER_SIZE];
    char response[BUFFER_SIZE];

    while (1) {
        printf("SQL> ");
        if (!fgets(query, sizeof(query), stdin)) break;

        if (strncmp(query, "exit", 4) == 0 || strncmp(query, "quit", 4) == 0) break;

        send(sockfd, query, strlen(query), 0);

        int len = read(sockfd, response, sizeof(response));
         printf("len=%d,\n",len);
        response[len] = '\0';
        if (len > 0) {
          
            printf("Response:\n%s\n", response);
        } else {
            printf("len=%d,Connection closed by server.\n",len);
            break;
        }
    }

    close(sockfd);
    return 0;
}
