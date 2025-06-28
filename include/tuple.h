#ifndef TUPLE_H
#define TUPLE_H
#include "catalog.h"
#include <stdint.h>
#include <stdbool.h>
#include "types.h"

// 创建新元组（基于表元数据）
Tuple* create_tuple(const TableMeta* meta, const void** values);

// 复制元组
Tuple* copy_tuple(const Tuple* src);

// 释放元组内存
void free_tuple(Tuple* tuple);

// 序列化元组
size_t serialize_tuple(const Tuple* tuple, uint8_t* buffer);

// 反序列化元组
size_t deserialize_tuple(Tuple* tuple, const uint8_t* buffer);

// 获取元组中指定列的值
void* tuple_get_value(const Tuple* tuple, uint8_t col_index);

// 设置元组中指定列的值
bool tuple_set_value(Tuple* tuple, uint8_t col_index, const void* value);

// 打印元组
//void print_tuple(const Tuple* tuple, const TableMeta* meta);
void print_tuple(const Tuple* tuple, const TableMeta* meta, uint32_t current_xid) ;
// 比较两个元组是否相等
bool tuple_equals(const Tuple* t1, const Tuple* t2);

// 计算元组的哈希值（用于索引）
uint32_t tuple_hash(const Tuple* tuple);
bool eval_condition(const Condition* cond, const Tuple* t, const TableMeta* meta) ;
bool is_tuple_visible(TransactionManager *txmgr,const Tuple* tuple, uint32_t current_xid) ;
#endif // TUPLE_H