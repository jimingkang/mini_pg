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
    stmt->num_columns = 3;
    strcpy(stmt->columns[0].name, "id");
    stmt->columns[0].type=INT4_TYPE;
    strcpy(stmt->columns[1].name, "name");
    stmt->columns[1].type=TEXT_TYPE;

      strcpy(stmt->columns[2].name, "age");
    stmt->columns[2].type=INT4_TYPE;
    return true;
}

bool parse_insert(const char* sql, InsertStmt* stmt) {
    // 模拟解析: insert into users values (1, 'Tom')
    strcpy(stmt->table_name, "users");
    stmt->num_values = 3;
    stmt->values[0] = strdup("20");
    stmt->values[1] = strdup("Tom");
      stmt->values[2] = strdup("38");
    return true;
}

bool parse_select(const char* sql, SelectStmt* stmt) {
    // 模拟解析: select id, name from users
    strcpy(stmt->table_name, "users");
    stmt->num_columns = 3;
    strcpy(stmt->columns[0], "id");
    strcpy(stmt->columns[1], "name");
    strcpy(stmt->columns[2], "age");
    return true;
}

bool parse_update(const char* sql, UpdateStmt*stmt) {
  
    strcpy(stmt->table_name, "users");
    stmt->num_assignments = 1;
    strcpy(stmt->columns[0], "age");
    //stmt->values[0]=malloc(128);
    //strcpy(stmt->values[0] ,"110");
    stmt->values[0]=strdup("110");

    // 模拟 where 条件：name = 'Tom'
    stmt->has_where = true;
    strcpy(stmt->where.column, "name");
    fprintf(stderr, ">>> in parse_update: stmt->where.column = [%s]\n", stmt->where.column);
    strcpy(stmt->where.op, "=");
    strcpy(stmt->where.value, "Jack");
    fprintf(stderr, ">>> in parse_update: stmt->where.value = [%s]\n", stmt->where.value);
    return true;
}