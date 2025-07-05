%name mini_sql_parser
%token_type { Token }
%extra_context { Parse* pParse }
%include {
#include <stdlib.h>
#include <string.h>
#include "sqliteInt.h"
}

%token SELECT INSERT INTO VALUES FROM WHERE

%token AND OR NOT NULL LIKE BETWEEN

%token INTEGER FLOAT STRING ID VARIABLE

%token COMMA SEMI DOT STAR LP RP EQ NE LT GT LE GE

%token PLUS MINUS SLASH CONCAT

// 起始符号
%start_symbol input

// 类型声明
%type input { void }
%type stmt_list { void }
%type stmt { void }
%type select_stmt { Select* }
%type insert_stmt { Insert* }
%type expr { Expr* }
%type expr_list { ExprList* }
%type column_list { IdList* }
%type table_name { SrcList* }
%type where_clause { Expr* }

input ::= stmt_list.
stmt_list ::= stmt_list SEMI stmt.
stmt_list ::= stmt.
stmt ::= select_stmt.  { /* 处理SELECT */ }
stmt ::= insert_stmt.  { /* 处理INSERT */ }

// SELECT语句
select_stmt(A) ::= SELECT expr_list(B) FROM table_name(C) where_clause(D). {
  A = sqlite3SelectNew(pParse, B, C, D, 0, 0, 0, 0, 0);
}

// INSERT语句
insert_stmt(A) ::= INSERT INTO table_name(B) column_list(C) VALUES expr_list(D). {
  A = sqlite3Insert(pParse, B, D, C, 0, 0);
}

// 表达式处理
expr(A) ::= ID(X). {
  A = sqlite3PExpr(pParse, TK_ID, 0, 0);
  if(A) A->u.zToken = sqlite3DbStrDup(pParse->db, X.z, X.n);
}
expr(A) ::= INTEGER(X). {
  A = sqlite3ExprAlloc(pParse->db, TK_INTEGER, &X, 0);
}
expr(A) ::= STRING(X). {
  A = sqlite3ExprAlloc(pParse->db, TK_STRING, &X, 0);
}
expr(A) ::= expr(B) PLUS expr(C). {
  A = sqlite3PExpr(pParse, TK_PLUS, B, C);
}
expr(A) ::= expr(B) EQ expr(C). {
  A = sqlite3PExpr(pParse, TK_EQ, B, C);
}
expr(A) ::= expr(B) AND expr(C). {
  A = sqlite3PExpr(pParse, TK_AND, B, C);
}

// 表达式列表
expr_list(A) ::= expr_list(B) COMMA expr(C). {
  A = sqlite3ExprListAppend(pParse, B, C);
}
expr_list(A) ::= expr(B). {
  A = sqlite3ExprListAppend(pParse, 0, B);
}

// 列列表
column_list(A) ::= LP id_list(B) RP. { A = B; }
id_list(A) ::= id_list(B) COMMA ID(C). {
  A = sqlite3IdListAppend(pParse, B, &C);
}
id_list(A) ::= ID(B). {
  A = sqlite3IdListAppend(pParse, 0, &B);
}

// 表名
table_name(A) ::= ID(B). {
  A = sqlite3SrcListAppend(pParse, 0, &B, 0);
}

// WHERE子句
where_clause(A) ::= WHERE expr(B). { A = B; }
where_clause(A) ::= . { A = 0; }

// 包含必要的错误处理
%syntax_error {
  sqlite3ErrorMsg(pParse, "Syntax error near line %d", pParse->sLastToken.z);
}
%parse_failure {
  sqlite3ErrorMsg(pParse, "Parse failed");
}
%stack_overflow {
  sqlite3ErrorMsg(pParse, "Parser stack overflow");
}