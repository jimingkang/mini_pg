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
void print_tuple(const Tuple* tuple, const TableMeta* meta, uint32_t current_xid) {
    if (!tuple) {
        printf("(NULL tuple)\n");
        return;
    }
    
    printf("Tuple OID: %u xmin: %u, xmax: %u,Columns: %d,current_xid: %d\n",tuple->oid, tuple->xmin, tuple->xmax,tuple->col_count,current_xid);

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




bool is_in_snapshot(uint32_t xid, const Snapshot *snap) {
    for (int i = 0; i < snap->active_count; i++) {
        if (snap->active_xids[i] == xid) return true;
    }
    return false;
}

bool has_newer_visible_version(TableMeta *meta, TransactionManager *txmgr, const Tuple *old_tuple, uint32_t current_xid,const Snapshot *  snap) {
    // 假设你能扫描全表（或快速索引某个 OID 所有版本）
    //for (PageID page_id = meta->first_page; page_id != INVALID_PAGE_ID; page_id = get_next_page_id(...)) {
    //    Page *page = load_page(meta, page_id);
    //    for (int i = 0; i < page->header.tuple_count; i++) {
    //        Tuple *t = &page->slots;
     for (PageID page_id = meta->first_page; page_id <= meta->last_page; page_id++) {

        Page *page = page_cache_load_or_fetch(page_id, meta);
        if (!page) continue;

        int orig_slot_count = page->header.slot_count;

        for (int i = 0; i < orig_slot_count; i++) {
            Slot *slot = &page->slots[i];
            if (slot->flags != SLOT_OCCUPIED) continue;
            Tuple *t = page_get_tuple(page, i, meta);

            // 1. 跳过自己（就是传进来的旧版本）
            if (t == old_tuple) continue;

            // 2. 必须是同一个 OID（表示是 update 后的同一逻辑行）
            if (t->oid != old_tuple->oid) continue;

            // 3. 如果是更“新的版本”，并且对当前事务可见
           // if (t->xmin > old_tuple->xmin &&
           //     is_tuple_visible(meta,txmgr, t, current_xid,snap)) {
           //     return true; // ✅ 找到更新版本
           // }

           // 是更新版本，且插入事务已提交
            if (t->xmin > old_tuple->xmin &&
                txmgr_is_committed(txmgr, t->xmin)) {
                return true;
            }
        }
    }

    return false; // ❌ 没有更可见的新版本
}
bool is_visible_by_snapshot(uint32_t xid, const Snapshot* snap) {
    return xid >= snap->xmin &&
           xid < snap->xmax &&
           !is_in_snapshot(xid, snap);
}
bool is_tuple_visible(TableMeta *meta, TransactionManager *txmgr, const Tuple *tuple, uint32_t current_xid, const Snapshot *snap) {


    // 1. 当前事务插入，未删除 → 可见
    if (tuple->xmin == current_xid && tuple->xmax == 0)
        return true;

    // 2. 插入事务未提交 → 不可见
    if (!txmgr_is_committed(txmgr, tuple->xmin))
    {
        printf("  → return false: reason : 插入事务未提交 → 不可见\n"); 
        return false;
        }


    // 3. 插入事务正在进行中或比当前快照还新 → 不可见
    if ( tuple->xmin >= snap->xmax || is_in_snapshot(tuple->xmin, snap) )
           {
        printf("  → return false: reason :3. 插入事务正在进行中或比当前快照还新 → 不可见\n"); 
        return false;
        }

    // 到这里说明该 tuple 的插入事务是 "可见的"

    // 4. 未被删除 → 可见（需检查是否有新版本）
    if (tuple->xmax == 0) {
        if (has_newer_visible_version(meta, txmgr, tuple, current_xid, snap))
            return false;
        return true;
    }

    // 5. 删除事务是当前事务 → 不可见
    if (tuple->xmax == current_xid)
        return false;

    // 6. 删除事务未提交 → 不可见（除非后续版本隐藏了它）
    if (!txmgr_is_committed(txmgr, tuple->xmax)) {
        if (has_newer_visible_version(meta, txmgr, tuple, current_xid, snap))
            return false;
        return true; // 删除事务未提交，自己可见
    }

    // 7. 删除事务在快照中或快照之后才提交 → 不可见
    //if ( tuple->xmax >= snap->xmax||is_in_snapshot(tuple->xmax, snap) )
     //   return false;
    // if (is_visible_by_snapshot(tuple->xmax, snap)) {
   // return true;  // 删除事务已经提交且不在 snapshot 中 → 可见
//}

 if (is_visible_by_snapshot(tuple->xmax, snap)) {
    return false;   // 删除事务在 snapshot 活跃区间 → 不可见
}
    // 8. 删除事务已提交，且早于 snapshot → 不可见
    if (tuple->xmax < snap->xmin)
        return false;

    // 9. 删除事务在 snapshot 可见范围 → 可见（说明删除操作对当前事务还没生效）
    return true;
}

bool back_is_tuple_visible(TableMeta *meta, TransactionManager *txmgr, const Tuple *tuple, uint32_t current_xid, const Snapshot *snap) {
 printf("[visible-debug] xid=%d checks tuple {xmin=%d, xmax=%d}, committed(xmin)=%d, committed(xmax)=%d, snap_xmin=%d, snap_xmax=%d\n",
        current_xid,
        tuple->xmin,
        tuple->xmax,
        txmgr_is_committed(txmgr, tuple->xmin),
        txmgr_is_committed(txmgr, tuple->xmax),
        snap->xmin,
       snap->xmax);
    // printf("current_xid:%d,snapshot_xmin:%d\n",current_xid,snapshot_xmin);
    // 1. 当前事务插入，未删除 → 可见
    if (tuple->xmin == current_xid && tuple->xmax == 0) return true;

    // 2. 插入事务未提交 → 不可见
    if (!txmgr_is_committed(txmgr, tuple->xmin)) return false;

     // 3. 插入事务虽提交，但比我的 snapshot 还新 → 不可见（不是我自己）
    //if (tuple->xmin > snapshot_xmin && tuple->xmin != current_xid) return false;

  // 2. 插入事务正在进行中（在快照中）→ 不可见
    if (is_in_snapshot(tuple->xmin, snap)) return false;
    // 3. 插入事务在快照之后才提交 → 不可见
    if (tuple->xmin >= snap->xmax) return false;

    // 到此为止，插入事务是已提交且符合 snapshot，说明“行曾存在”

    // 4. 行未被删除 → 可见
    //if (tuple->xmax == 0) {return true;}
      if (tuple->xmax == 0) { 
         if (has_newer_visible_version(meta, txmgr, tuple, current_xid, snap)) {
        return false;  // 存在更可见的新版本，旧版本不可见
    }
    return true;  // 没有新版本，旧版本可见
    }

    // 5. 删除事务是当前事务 → 不可见
    if (tuple->xmax == current_xid) return false;

    // 6. 删除事务未提交
    if (!txmgr_is_committed(txmgr, tuple->xmax)) {
        // 有更高版本（如 update 插入的行）且可见 → 当前版本不可见
        if (has_newer_visible_version(meta, txmgr, tuple, current_xid, snap)) {
            return false;
        }
        return true;
    }

     // 7. 删除事务在快照中（活跃）→ 不可见
    if (is_in_snapshot(tuple->xmax, snap)) return false;

    // 8. 删除事务在快照之后才提交 → 不可见
    if (tuple->xmax >= snap->xmax) return false;

    // 7. 删除事务已提交，且早于我的快照 → 不可见
    //if (tuple->xmax <= snap->xmax) return false;


      // 删除事务也要判断
    if (tuple->xmax != 0) {
        if (txmgr_is_committed(txmgr,tuple->xmax)) {
            if (tuple->xmax <= snap->xmin) return false;
        } else {
            // 删除事务未提交，还要检查是否存在新版本（版本链或扫描）
            if (has_newer_visible_version(meta, txmgr, tuple, current_xid, snap)) return false;
        }
    }
        

    // 8. 删除事务提交时间在我之后 → 删除尚未生效 → 可见


    return true;
}


bool eval_condition(const MiniExprList* cond, const Tuple* t, const TableMeta* meta) {
    fprintf(stderr, "eval_condition: tuple id=%d, name=%s\n",
            t->columns[0].value.int_val, t->columns[1].value.str_val);

    for (int i = 0; i < t->col_count; i++) {
        //fprintf(stderr, "Checking condition: target column='%s', condition column='%s'\n", meta->cols[i].name, cond->column);

        if (strcmp(meta->cols[i].name, cond->items[0]->left->value) == 0) {
           // fprintf(stderr, "Column match found. Comparing with operator '%s'\n", cond->op);

           // if (strcmp(cond->items[0]->op, "=") == 0) {
              if (cond->items[0]->op == OP_EQ) {  
                if (meta->cols[i].type == TEXT_TYPE) {
                    fprintf(stderr, "Comparing TEXT: '%s' == '%s'\n",t->columns[i].value.str_val, cond->items[0]->right->value);
                   
                    return strcmp(t->columns[i].value.str_val, cond->items[0]->right->value) == 0;
                } else if (meta->cols[i].type == INT4_TYPE) {
                    int cond_val = atoi(cond->items[0]->right->value);
                    fprintf(stderr, "Comparing INT: %d == %d\n", t->columns[i].value.int_val, cond_val);
                    return t->columns[i].value.int_val == cond_val;
                } else {
                    fprintf(stderr, "Unsupported column type: %d\n", meta->cols[i].type);
                }
            } else {
                fprintf(stderr, "Unsupported operator: '%s'\n", cond->items[0]->op);
            }
        }
    }

    fprintf(stderr, "No matching column found for condition.\n");
    return false;
}
bool new_eval_expr(const MiniExpr* expr, const Tuple* t, const TableMeta* meta) {
    if (!expr) return true;

    switch (expr->type) {
        case EXPR_LITERAL:
        case EXPR_COLUMN:
            return true; // 不单独使用，略过

        case EXPR_BINARY: {
            MiniExpr* left = expr->left;
            MiniExpr* right = expr->right;

            switch (expr->op) {
                case OP_EQ:
                case OP_GT:
                case OP_LT: {
                    if (!left || !right) return false;
                    if (left->type != EXPR_COLUMN || right->type != EXPR_LITERAL) return false;

                    const char* col_name = left->value;
                    int col_idx = -1;
                    for (int i = 0; i < meta->col_count; ++i) {
                        if (strcmp(meta->cols[i].name, col_name) == 0) {
                            col_idx = i;
                            break;
                        }
                    }
                    if (col_idx == -1) return false;

                    DataType type = meta->cols[col_idx].type;
                    const OldColumn* col = &t->columns[col_idx];
                    const char* val_str = right->value;

                    switch (expr->op) {
                        case OP_EQ:
                            return (type == INT4_TYPE)
                                ? col->value.int_val == atoi(val_str)
                                : strcmp(col->value.str_val, val_str) == 0;
                        case OP_GT:
                            return col->value.int_val > atoi(val_str);
                        case OP_LT:
                            return col->value.int_val < atoi(val_str);
                    }
                    break;
                }

                case OP_AND:
                    return eval_expr(left, t, meta) && eval_expr(right, t, meta);
                case OP_OR:
                    return eval_expr(left, t, meta) || eval_expr(right, t, meta);
                default:
                    return false;
            }
        }

        default:
            return false;
    }
}


bool eval_expr(const MiniExpr* expr, const Tuple* t, const TableMeta* meta) {
    if (!expr) return true;

    switch (expr->type) {
        case EXPR_LITERAL:
            // LITERAL 本身不能单独判断，只用于二元对比
            return true;

        case EXPR_COLUMN:
            // COLUMN 也不能单独判断，只出现在二元左侧
            return true;

        case EXPR_BINARY: {
            // 递归计算左右
            const MiniExpr* left = expr->left;
            const MiniExpr* right = expr->right;
            if (!left || !right) return false;

            // 左侧：字段名
            if (left->type != EXPR_COLUMN) return false;
            const char* col_name = left->value;
            int col_idx = -1;
            for (int i = 0; i < meta->col_count; ++i) {
                if (strcmp(meta->cols[i].name, col_name) == 0) {
                    col_idx = i;
                    break;
                }
            }
            if (col_idx == -1) return false;

            DataType type = meta->cols[col_idx].type;
            const OldColumn* col = &t->columns[col_idx];

            // 右侧是字面值
            const char* val_str = right->value;

            // 做比较
            switch (expr->op) {
                case OP_EQ:
                    if (type == INT4_TYPE)
                        return col->value.int_val == atoi(val_str);
                    else if (type == TEXT_TYPE)
                        return strcmp(col->value.str_val, val_str) == 0;
                    break;

                case OP_GT:
                    if (type == INT4_TYPE)
                        return col->value.int_val > atoi(val_str);
                    break;

                case OP_LT:
                    if (type == INT4_TYPE)
                        return col->value.int_val < atoi(val_str);
                    break;

                case OP_AND:
                    return eval_expr(left, t, meta) && eval_expr(right, t, meta);

                case OP_OR:
                    return eval_expr(left, t, meta) || eval_expr(right, t, meta);

                default:
                    return false;
            }
            return false;
        }

        default:
            return false;
    }
}

bool convertToTuple(Tuple* tuple, TableMeta* meta, InsertStmt* insert) {
    if (!tuple || !meta || !insert) return false;

    tuple->col_count = insert->column_count;
    tuple->columns = (OldColumn*)malloc(sizeof(OldColumn) * tuple->col_count);
    if (!tuple->columns) return false;

    for (int i = 0; i < insert->column_count; ++i) {
        const char* col_name = insert->column_names[i];
        MiniExpr* val_expr = insert->values->items[i];

       // if (!val_expr || val_expr->type != EXPR_LITERAL) {
        if (!val_expr->value || val_expr->type != EXPR_LITERAL) {
            fprintf(stderr, "[convertToTuple] 第 %d 列不是字面量，暂不支持表达式插入\n", i);
            return false;
        }

        const char* val_str = val_expr->value;

        // 在表元信息中找到列索引
        int col_idx = -1;
        for (int j = 0; j < meta->col_count; ++j) {
            if (strcmp(meta->cols[j].name, col_name) == 0) {
                col_idx = j;
                break;
            }
        }

        if (col_idx == -1) {
            fprintf(stderr, "[convertToTuple] 列 '%s' 不存在于表 '%s'\n", col_name, meta->name);
            return false;
        }

        // 按表定义的类型填充 tuple
        DataType type = meta->cols[col_idx].type;
        tuple->columns[i].type = type;

        if (type == INT4_TYPE) {
            tuple->columns[i].value.int_val = atoi(val_str);
        } else if (type == TEXT_TYPE) {
            tuple->columns[i].value.str_val = malloc(MAX_STRING_LEN);
            if (!tuple->columns[i].value.str_val) {
                fprintf(stderr, "malloc failed\n");
                return false;
            }
            strncpy(tuple->columns[i].value.str_val, val_str, MAX_STRING_LEN - 1);
            tuple->columns[i].value.str_val[MAX_STRING_LEN - 1] = '\0';
           // strncpy(tuple->columns[i].value.str_val, val_str, MAX_STRING_LEN);
        } else {
            fprintf(stderr, "[convertToTuple] 不支持的类型列 '%s'\n", col_name);
            return false;
        }
    }

    return true;
}

void format_tuple(char* buf, size_t buf_size, Tuple* tuple, TableMeta* meta, uint32_t current_xid) {
    size_t offset = 0;

    for (int i = 0; i < tuple->col_count; ++i) {
        if (meta->cols[i].type == INT4_TYPE) {
            offset += snprintf(buf + offset, buf_size - offset, "%d\t", tuple->columns[i].value.int_val);
        } else if (meta->cols[i].type == TEXT_TYPE) {
            offset += snprintf(buf + offset, buf_size - offset, "%s\t", tuple->columns[i].value.str_val);
        } else {
            offset += snprintf(buf + offset, buf_size - offset, "<UNKNOWN>\t");
        }
    }

    snprintf(buf + offset, buf_size - offset, "\n");
}

