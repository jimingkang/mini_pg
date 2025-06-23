// parser.c
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "server/parser.h"
#include "types.h"

bool parse_create_table(const char* sql, CreateTableStmt* stmt) {
    // 模拟解析: create table users (id INT, name TEXT)
    strcpy(stmt->table_name, "users");
    stmt->num_columns = 2;
    strcpy(stmt->columns[0].name, "id");
    stmt->columns[0].type=INT4_TYPE;
    strcpy(stmt->columns[1].name, "name");
    stmt->columns[1].type=TEXT_TYPE;
    return true;
}

bool parse_insert(const char* sql, InsertStmt* stmt) {
    // 模拟解析: insert into users values (1, 'Tom')
    strcpy(stmt->table_name, "users");
    stmt->num_values = 2;
    stmt->values[0] = strdup("1");
    stmt->values[1] = strdup("Tom");
    return true;
}

bool parse_select(const char* sql, SelectStmt* stmt) {
    // 模拟解析: select id, name from users
    strcpy(stmt->table_name, "users");
    stmt->num_columns = 2;
    strcpy(stmt->columns[0], "id");
    strcpy(stmt->columns[1], "name");
    return true;
}
