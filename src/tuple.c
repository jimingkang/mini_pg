#include "tuple.h"
#include "catalog.h"
#include "parser.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

// 创建新元组
Tuple* create_tuple(const TableMeta* meta, const void** values) {
    if (!meta || meta->col_count == 0) return NULL;
    
    // 分配元组内存
    Tuple* tuple = (Tuple*)malloc(sizeof(Tuple));
    if (!tuple) return NULL;
    
    // 初始化元组头
    tuple->oid = 0;         // 由系统分配
    tuple->xmin = 0;        // 事务开始时设置
    tuple->xmax = 0;        // 事务结束时设置
    tuple->deleted = false;
    tuple->col_count = meta->col_count;
    
    // 分配列数组
    tuple->columns = (Column*)malloc(meta->col_count * sizeof(Column));
    if (!tuple->columns) {
        free(tuple);
        return NULL;
    }
    
    // 初始化每列数据
    for (int i = 0; i < meta->col_count; i++) {
        tuple->columns[i].type = meta->cols[i].type;

        if (!values || !values[i])
        {
            // 没有提供值，设置默认值
            switch (tuple->columns[i].type) {
                case INT4_TYPE:
                    tuple->columns[i].value.int_val = 0;
                    break;
                case FLOAT_TYPE:
                    tuple->columns[i].value.float_val = 0.0f;
                    break;
                case BOOL_TYPE:
                    tuple->columns[i].value.bool_val = false;
                    break;
                case TEXT_TYPE:
                    tuple->columns[i].value.str_val = strdup("");
                    if (!tuple->columns[i].value.str_val) {
                        perror("Failed to allocate empty string");
                        // 清理已分配的资源
                        for (int j = 0; j < i; j++) {
                            if (tuple->columns[j].type == TEXT_TYPE) {
                                free(tuple->columns[j].value.str_val);
                            }
                        }
                        free(tuple->columns);
                        free(tuple);
                        return NULL;
                    }
                    break;
                case DATE_TYPE:
                    tuple->columns[i].value.int_val = (int32_t)time(NULL);
                    break;
                default:
                    tuple->columns[i].value.int_val = 0;
            }
        }
        else
        {
            // 设置提供的值
            switch (tuple->columns[i].type) {
                case INT4_TYPE:
                    tuple->columns[i].value.int_val = *((const int32_t*)values[i]);
                    break;
                case FLOAT_TYPE:
                    tuple->columns[i].value.float_val = *((const float*)values[i]);
                    break;
                case BOOL_TYPE:
                    tuple->columns[i].value.bool_val = *((const bool*)values[i]);
                    break;
                case TEXT_TYPE: {
                    const char* str = (const char*)values[i];
                    tuple->columns[i].value.str_val = strdup(str);
                    break;
                }
                case DATE_TYPE:
                    tuple->columns[i].value.int_val = *((const int32_t*)values[i]);
                    break;
                default:
                    // 未知类型，设置为0
                    tuple->columns[i].value.int_val = 0;
            }
        }
    }
    
    return tuple;
}

// 复制元组
Tuple* copy_tuple(const Tuple* src) {
    if (!src) return NULL;
    
    Tuple* dest = (Tuple*)malloc(sizeof(Tuple));
    if (!dest) return NULL;
    
    // 复制元组头
    dest->oid = src->oid;
    dest->xmin = src->xmin;
    dest->xmax = src->xmax;
    dest->deleted = src->deleted;
    dest->col_count = src->col_count;
    
    // 分配列数组
    dest->columns = (Column*)malloc(dest->col_count * sizeof(Column));
    if (!dest->columns) {
        free(dest);
        return NULL;
    }
    
    // 复制每列数据
    for (int i = 0; i < dest->col_count; i++) {
        dest->columns[i].type = src->columns[i].type;
        
        switch (src->columns[i].type) {
            case TEXT_TYPE:
                // 字符串需要深度复制
                dest->columns[i].value.str_val = strdup(src->columns[i].value.str_val);
                break;
            default:
                // 其他类型直接复制值
                dest->columns[i].value = src->columns[i].value;
        }
    }
    
    return dest;
}

// 释放元组
void free_tuple(Tuple* tuple) {
    if (tuple) {
        if (tuple->columns) {
            // 释放所有字符串内存
            for (int i = 0; i < tuple->col_count; i++) {
                if (tuple->columns[i].type == TEXT_TYPE && 
                    tuple->columns[i].value.str_val) {
                    free(tuple->columns[i].value.str_val);
                }
            }
            free(tuple->columns);
        }
        free(tuple);
    }
}

// 序列化元组
size_t serialize_tuple(const Tuple* tuple, uint8_t* buffer) {
    if (!tuple || !buffer) return 0;
    
    uint8_t* ptr = buffer;
    
    // 序列化元组头
    memcpy(ptr, &tuple->oid, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, &tuple->xmin, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, &tuple->xmax, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    *ptr++ = tuple->deleted ? 1 : 0;
    *ptr++ = tuple->col_count;
    
    // 序列化每列数据
    for (int i = 0; i < tuple->col_count; i++) {
        *ptr++ = (uint8_t)tuple->columns[i].type;
        
        switch (tuple->columns[i].type) {
            case INT4_TYPE:
            case DATE_TYPE:
                memcpy(ptr, &tuple->columns[i].value.int_val, sizeof(int32_t));
                ptr += sizeof(int32_t);
                break;
                
            case FLOAT_TYPE:
                memcpy(ptr, &tuple->columns[i].value.float_val, sizeof(float));
                ptr += sizeof(float);
                break;
                
            case BOOL_TYPE:
                *ptr++ = tuple->columns[i].value.bool_val ? 1 : 0;
                break;
                
            case TEXT_TYPE: {
                const char* str = tuple->columns[i].value.str_val;
                size_t len = str ? strlen(str) : 0;
                
                // 写入字符串长度（16位）
                uint16_t len16 = (uint16_t)len;
                memcpy(ptr, &len16, sizeof(uint16_t));
                ptr += sizeof(uint16_t);
                
                // 写入字符串内容
                if (len > 0) {
                    memcpy(ptr, str, len);
                    ptr += len;
                }
                break;
            }
        }
    }
    
    return ptr - buffer;
}

// 反序列化元组
size_t deserialize_tuple(Tuple* tuple, const uint8_t* buffer) {
    if (!tuple || !buffer) return 0;
    
    const uint8_t* ptr = buffer;
    
    // 反序列化元组头
    memcpy(&tuple->oid, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(&tuple->xmin, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(&tuple->xmax, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    tuple->deleted = *ptr++ != 0;
    tuple->col_count = *ptr++;
    
    // 分配列数组
    tuple->columns = (Column*)malloc(tuple->col_count * sizeof(Column));
    if (!tuple->columns) return 0;
    
    // 反序列化每列数据
    for (int i = 0; i < tuple->col_count; i++) {
        tuple->columns[i].type = (DataType)*ptr++;
        
        switch (tuple->columns[i].type) {
            case INT4_TYPE:
            case DATE_TYPE:
                memcpy(&tuple->columns[i].value.int_val, ptr, sizeof(int32_t));
                ptr += sizeof(int32_t);
                break;
                
            case FLOAT_TYPE:
                memcpy(&tuple->columns[i].value.float_val, ptr, sizeof(float));
                ptr += sizeof(float);
                break;
                
            case BOOL_TYPE:
                tuple->columns[i].value.bool_val = *ptr++ != 0;
                break;
                
            case TEXT_TYPE: {
                uint16_t len;
                memcpy(&len, ptr, sizeof(uint16_t));
                ptr += sizeof(uint16_t);
                
                // 分配字符串内存
                tuple->columns[i].value.str_val = (char*)malloc(len + 1);
                if (!tuple->columns[i].value.str_val) {
                    // 内存分配失败，清理已分配的资源
                    for (int j = 0; j < i; j++) {
                        if (tuple->columns[j].type == TEXT_TYPE) {
                            free(tuple->columns[j].value.str_val);
                        }
                    }
                    free(tuple->columns);
                    tuple->columns = NULL;
                    return 0;
                }
                
                // 复制字符串内容
                if (len > 0) {
                    memcpy(tuple->columns[i].value.str_val, ptr, len);
                }
                tuple->columns[i].value.str_val[len] = '\0';
                ptr += len;
                break;
            }
        }
    }
    
    return ptr - buffer;
}

// 获取元组值
void* tuple_get_value(const Tuple* tuple, uint8_t col_index) {
    if (!tuple || col_index >= tuple->col_count) {
        return NULL;
    }
    
    switch (tuple->columns[col_index].type) {
        case INT4_TYPE:
        case DATE_TYPE:
            return (void*)&tuple->columns[col_index].value.int_val;
        case FLOAT_TYPE:
            return (void*)&tuple->columns[col_index].value.float_val;
        case BOOL_TYPE:
            return (void*)&tuple->columns[col_index].value.bool_val;
        case TEXT_TYPE:
            return (void*)tuple->columns[col_index].value.str_val;
        default:
            return NULL;
    }
}

// 设置元组值
bool tuple_set_value(Tuple* tuple, uint8_t col_index, const void* value) {
    if (!tuple || !value || col_index >= tuple->col_count) {
        return false;
    }
    
    switch (tuple->columns[col_index].type) {
        case INT4_TYPE:
        case DATE_TYPE:
            tuple->columns[col_index].value.int_val = *((const int32_t*)value);
            return true;
            
        case FLOAT_TYPE:
            tuple->columns[col_index].value.float_val = *((const float*)value);
            return true;
            
        case BOOL_TYPE:
            tuple->columns[col_index].value.bool_val = *((const bool*)value);
            return true;
            
        case TEXT_TYPE: {
            const char* str = (const char*)value;
            size_t new_len = strlen(str);
            
            // 释放旧字符串
            if (tuple->columns[col_index].value.str_val) {
                free(tuple->columns[col_index].value.str_val);
            }
            
            // 分配并复制新字符串
            char* new_str = strdup(str);
            if (!new_str) return false;
            
            tuple->columns[col_index].value.str_val = new_str;
            return true;
        }
            
        default:
            return false;
    }
}

// 打印元组
void print_tuple(const Tuple* tuple, const TableMeta* meta) {
    if (!tuple) {
        printf("(NULL tuple)\n");
        return;
    }
    
    printf("Tuple OID: %u\n", tuple->oid);
    printf("xmin: %u, xmax: %u\n", tuple->xmin, tuple->xmax);
    printf("Deleted: %s\n", tuple->deleted ? "true" : "false");
    printf("Columns: %d\n", tuple->col_count);
    
    for (int i = 0; i < tuple->col_count; i++) {
        const char* col_name = meta && i < meta->col_count ? 
                              meta->cols[i].name : "Unknown";
        
        printf("  %s (%d): ", col_name, tuple->columns[i].type);
        
        switch (tuple->columns[i].type) {
            case INT4_TYPE:
                printf("%d", tuple->columns[i].value.int_val);
                break;
            case FLOAT_TYPE:
                printf("%.2f", tuple->columns[i].value.float_val);
                break;
            case BOOL_TYPE:
                printf("%s", tuple->columns[i].value.bool_val ? "true" : "false");
                break;
            case TEXT_TYPE:
                printf("\"%s\"", tuple->columns[i].value.str_val);
                break;
            case DATE_TYPE: {
                time_t t = (time_t)tuple->columns[i].value.int_val;
                char buf[32];
                strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&t));
                printf("%s", buf);
                break;
            }
            default:
                printf("(unknown type)");
        }
        printf("\n");
    }
    printf("\n");
}

// 比较两个元组是否相等
bool tuple_equals(const Tuple* t1, const Tuple* t2) {
    if (!t1 || !t2) return false;
    if (t1 == t2) return true;
    
    // 检查基本信息
    if (t1->oid != t2->oid || 
        t1->col_count != t2->col_count || 
        t1->deleted != t2->deleted) {
        return false;
    }
    
    // 比较每列数据
    for (int i = 0; i < t1->col_count; i++) {
        if (t1->columns[i].type != t2->columns[i].type) {
            return false;
        }
        
        switch (t1->columns[i].type) {
            case INT4_TYPE:
            case DATE_TYPE:
                if (t1->columns[i].value.int_val != t2->columns[i].value.int_val)
                    return false;
                break;
                
            case FLOAT_TYPE:
                if (t1->columns[i].value.float_val != t2->columns[i].value.float_val)
                    return false;
                break;
                
            case BOOL_TYPE:
                if (t1->columns[i].value.bool_val != t2->columns[i].value.bool_val)
                    return false;
                break;
                
            case TEXT_TYPE:
                if (strcmp(t1->columns[i].value.str_val, t2->columns[i].value.str_val) != 0)
                    return false;
                break;
                
            default:
                // 未知类型，视为不相等
                return false;
        }
    }
    
    return true;
}

// 计算元组的哈希值
uint32_t tuple_hash(const Tuple* tuple) {
    if (!tuple) return 0;
    
    uint32_t hash = 5381;
    
    // 哈希基本信息
    hash = ((hash << 5) + hash) + tuple->oid;
    hash = ((hash << 5) + hash) + tuple->xmin;
    hash = ((hash << 5) + hash) + tuple->xmax;
    hash = ((hash << 5) + hash) + tuple->deleted;
    hash = ((hash << 5) + hash) + tuple->col_count;
    
    // 哈希每列数据
    for (int i = 0; i < tuple->col_count; i++) {
        hash = ((hash << 5) + hash) + tuple->columns[i].type;
        
        switch (tuple->columns[i].type) {
            case INT4_TYPE:
            case DATE_TYPE:
                hash = ((hash << 5) + hash) + tuple->columns[i].value.int_val;
                break;
                
            case FLOAT_TYPE:
                // 将浮点数转换为整数进行哈希
                hash = ((hash << 5) + hash) + *((uint32_t*)&tuple->columns[i].value.float_val);
                break;
                
            case BOOL_TYPE:
                hash = ((hash << 5) + hash) + (tuple->columns[i].value.bool_val ? 1 : 0);
                break;
                
            case TEXT_TYPE: {
                const char* str = tuple->columns[i].value.str_val;
                if (str) {
                    while (*str) {
                        hash = ((hash << 5) + hash) + *str++;
                    }
                }
                break;
            }
        }
    }
    
    return hash;
}

bool is_tuple_visible(const Tuple* tuple, uint32_t current_xid) {
    // 最简单的 MVCC 可见性判断（简化版）
    return tuple->xmin <= current_xid && tuple->xmax == 0;
}
bool old_eval_condition(const Condition* cond, const Tuple* t, const TableMeta* meta) {
       fprintf(stderr, "eval_condition: tuple id=%d,name=%s\n", t->columns[0].value.int_val,t->columns[1].value.str_val);
    for (int i = 0; i < t->col_count; i++) {
        if (strcmp(meta->cols[i].name, cond->column) == 0) {
            if (strcmp(cond->op, "=") == 0) {
               if (meta->cols[i].type == TEXT_TYPE) {
                fprintf(stderr, "meta->cols[i].type =%d\n",meta->cols[i].type );
                return strcmp(t->columns[i].value.str_val, cond->value) == 0;
               } else if (meta->cols[i].type == INT4_TYPE) {
                int cond_val = atoi(cond->value);
                fprintf(stderr, "cond_val =%d\n",cond_val );
                return t->columns[i].value.int_val == cond_val;
               }
            }
        }
    }
    return false;
}
bool eval_condition(const Condition* cond, const Tuple* t, const TableMeta* meta) {
    fprintf(stderr, "eval_condition: tuple id=%d, name=%s\n",
            t->columns[0].value.int_val, t->columns[1].value.str_val);

    for (int i = 0; i < t->col_count; i++) {
        //fprintf(stderr, "Checking condition: target column='%s', condition column='%s'\n", meta->cols[i].name, cond->column);

        if (strcmp(meta->cols[i].name, cond->column) == 0) {
           // fprintf(stderr, "Column match found. Comparing with operator '%s'\n", cond->op);

            if (strcmp(cond->op, "=") == 0) {
                if (meta->cols[i].type == TEXT_TYPE) {
                    fprintf(stderr, "Comparing TEXT: '%s' == '%s'\n",t->columns[i].value.str_val, cond->value);
                    return strcmp(t->columns[i].value.str_val, cond->value) == 0;
                } else if (meta->cols[i].type == INT4_TYPE) {
                    int cond_val = atoi(cond->value);
                    fprintf(stderr, "Comparing INT: %d == %d\n", t->columns[i].value.int_val, cond_val);
                    return t->columns[i].value.int_val == cond_val;
                } else {
                    fprintf(stderr, "Unsupported column type: %d\n", meta->cols[i].type);
                }
            } else {
                fprintf(stderr, "Unsupported operator: '%s'\n", cond->op);
            }
        }
    }

    fprintf(stderr, "No matching column found for condition.\n");
    return false;
}
