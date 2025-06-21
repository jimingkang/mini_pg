// types.h
#ifndef TYPES_H
#define TYPES_H

#include <stdint.h>
#include <stdbool.h>



// 页面ID类型
#define PAGE_SIZE 4096   // 4KB
#define PAGE_HEADER_SIZE 32
#define INVALID_PAGE_ID 0

#define MAX_NAME_LEN 50
#define MAX_TABLES 100
#define MAX_COLS 32




#define MAX_TUPLES_PER_PAGE 64
#define INVALID_PAGE_ID UINT32_MAX
#define MAX_RESULTS 1000  // 添加 MAX_RESULTS 定义
typedef uint32_t PageID;

#define MAX_SLOTS 64
#define SLOT_ARRAY_SIZE (MAX_SLOTS * sizeof(Slot))
#define PAGE_DATA_SIZE (PAGE_SIZE - sizeof(PageHeader))
#define SLOT_OCCUPIED 0x01
#define SLOT_DELETED  0x02
#define MAX_TUPLE_SIZE (PAGE_DATA_SIZE / 2)
#define INVALID_SLOT 0xFFFF

// 数据类型枚举
typedef enum {
    INT4_TYPE,      // 4字节整数
    FLOAT_TYPE,     // 4字节浮点数
    BOOL_TYPE,      // 1字节布尔值
    TEXT_TYPE,      // 变长字符串
    DATE_TYPE,      // 4字节日期（时间戳）
    // 可扩展更多类型...
} DataType;
// 列值联合体
typedef union {
    int32_t int_val;    // 整数值
    float float_val;    // 浮点数值
    bool bool_val;      // 布尔值
    char* str_val;      // 字符串值（动态分配）
} ColumnValue;

// 列结构
typedef struct {
    DataType type;      // 数据类型
    ColumnValue value;  // 列值
} Column;

// 列定义
typedef struct {
    char name[MAX_NAME_LEN];
    DataType type;
} ColumnDef;

// 元组结构
typedef struct {
    uint32_t oid;         // 元组唯一ID
    uint32_t xmin;        // 创建事务ID (MVCC)
    uint32_t xmax;        // 删除/更新事务ID (MVCC)
    bool deleted;         // 逻辑删除标志
    uint8_t col_count;    // 列数量
    Column* columns;      // 列数据数组
} Tuple;



// 页头结构
/*
typedef struct PageHeader {
    uint32_t checksum;
    PageID page_id;       // 页面ID
    uint32_t lsn;         // 最后修改的LSN
    uint16_t free_space;  // 空闲空间大小（字节）
    uint16_t tuple_count; // 元组数量
    PageID next_page;     // 下一页ID
    PageID prev_page;     // 上一页ID - 添加缺失的成员
} PageHeader;

 typedef struct Page {
    PageHeader header;
    Tuple tuples[MAX_TUPLES_PER_PAGE]; // 元组数组
    
} Page;

*/


typedef struct PageHeader {
    uint32_t checksum;
    PageID page_id;
    uint32_t lsn;
    uint16_t free_start;     // 数据区当前偏移
        uint16_t free_end;     // 数据区当前偏移

    uint16_t free_space;     // 剩余空间
    uint16_t tuple_count;    // 有效元组数量
    uint16_t slot_count;     // 槽位使用数量
    PageID next_page;
    PageID prev_page;
} PageHeader;
// 页面结构

typedef struct Slot {
    uint16_t offset;        // 数据区偏移
    uint16_t length;        // 数据长度
    uint16_t tuple_size;    // 原始元组大小
    uint8_t status;         // 槽位状态: SLOT_FREE, SLOT_USED, SLOT_DELETED
    uint8_t flags;      // 状态标志 (OCCUPIED/DELETED)
} Slot;

typedef struct Page {
    PageHeader header;
    Slot slots[MAX_SLOTS];          // 槽位数组
    uint8_t data[PAGE_DATA_SIZE];   // 数据区
} Page;


// 表元数据
typedef struct {
    uint32_t oid;
    char name[MAX_NAME_LEN];
    char filename[MAX_NAME_LEN];
    uint8_t col_count;
    ColumnDef cols[MAX_COLS];
    PageID first_page;   // 表的第一个页面ID
PageID last_page;    // 表的最后一个页面ID

} TableMeta;

#endif // TYPES_H