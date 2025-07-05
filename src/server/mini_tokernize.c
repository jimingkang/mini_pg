// 文件：mini_tokenizer.c
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "lemon_parser.h" // Lemon parser 生成头文件

// Token 类型枚举（与 %token 定义一致）
enum {
    TK_ID = 1, TK_STRING, TK_INT,
    TK_SELECT, TK_INSERT, TK_INTO, TK_VALUES,
    TK_FROM, TK_WHERE,
    TK_COMMA, TK_LP, TK_RP, TK_STAR, TK_EQ, TK_SEMI
};

typedef struct {
    const char* input;
    int pos;
} Tokenizer;

static void skip_whitespace(Tokenizer* tz) {
    while (isspace(tz->input[tz->pos])) tz->pos++;
}

static int is_keyword(const char* s, const char* kw) {
    return strcasecmp(s, kw) == 0;
}

static int scan_identifier(Tokenizer* tz, Token* out) {
    int start = tz->pos;
    while (isalnum(tz->input[tz->pos]) || tz->input[tz->pos] == '_') tz->pos++;
    int len = tz->pos - start;
    char* text = strndup(&tz->input[start], len);

    if (is_keyword(text, "select")) {*out = (Token){ .z = text }; return TK_SELECT;}
    if (is_keyword(text, "insert")) {*out = (Token){ .z = text }; return TK_INSERT;}
    if (is_keyword(text, "into")) {*out = (Token){ .z = text }; return TK_INTO;}
    if (is_keyword(text, "values")) {*out = (Token){ .z = text }; return TK_VALUES;}
    if (is_keyword(text, "from")) {*out = (Token){ .z = text }; return TK_FROM;}
    if (is_keyword(text, "where")) {*out = (Token){ .z = text }; return TK_WHERE;}

    *out = (Token){ .z = text };
    return TK_ID;
}

static int scan_number(Tokenizer* tz, Token* out) {
    int start = tz->pos;
    while (isdigit(tz->input[tz->pos])) tz->pos++;
    int len = tz->pos - start;
    out->z = strndup(&tz->input[start], len);
    return TK_INT;
}

static int scan_string(Tokenizer* tz, Token* out) {
    tz->pos++; // skip initial '
    int start = tz->pos;
    while (tz->input[tz->pos] && tz->input[tz->pos] != '\'' ) tz->pos++;
    int len = tz->pos - start;
    out->z = strndup(&tz->input[start], len);
    tz->pos++; // skip closing '
    return TK_STRING;
}

void run_tokenizer_and_parse(const char* sql) {
    Tokenizer tz = { sql, 0 };
    ParserContext ctx = {0};
    void* parser = mini_pg_parserAlloc(malloc);

    Token token;
    while (1) {
        skip_whitespace(&tz);
        char c = sql[tz.pos];
        if (c == 0) break;

        int tokenType = 0;
        token.z = NULL; token.n = 0;
        if (isalpha(c) || c == '_') tokenType = scan_identifier(&tz, &token);
        else if (isdigit(c))         tokenType = scan_number(&tz, &token);
        else if (c == '\'')         tokenType = scan_string(&tz, &token);
        else {
            tz.pos++;
            token.z = strndup(&c, 1);
            switch (c) {
                case ',': tokenType = TK_COMMA; break;
                case '(': tokenType = TK_LP; break;
                case ')': tokenType = TK_RP; break;
                case '*': tokenType = TK_STAR; break;
                case '=': tokenType = TK_EQ; break;
                case ';': tokenType = TK_SEMI; break;
                default: tokenType = 0; break;
            }
        }

        if (tokenType) {
            mini_pg_parser(parser, tokenType, token, &ctx);
        } else {
            printf("Unknown token at: %s\n", &sql[tz.pos]);
            break;
        }
    }

    mini_pg_parser(parser, 0, token, &ctx); // 发送结束符
    mini_pg_parserFree(parser, free);

    if (ctx.result) {
        printf("SQL parsed successfully.\n");
        // 你可以在这里打印 ctx.result 的内容
    } else {
        printf("SQL parse failed.\n");
    }
}
