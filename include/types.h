// types.h
#ifndef TYPES_H
#define TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <bits/pthreadtypes.h>

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>



// 页面ID类型
#define PAGE_SIZE 4096   // 4KB
#define PAGE_HEADER_SIZE 32
#define INVALID_PAGE_ID 0xFFFFFFFF

#define MAX_NAME_LEN 50
#define MAX_TABLES 100
#define MAX_COLS 32

#define MAX_TEXT_LEN 256 



#define MAX_TUPLES_PER_PAGE 64
//#define INVALID_PAGE_ID UINT32_MAX
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

// 事务状态
typedef enum {
    TRANS_ACTIVE,     // 事务进行中
    TRANS_COMMITTED,  // 事务已提交
    TRANS_ABORTED     // 事务已中止
} TransactionState;
typedef struct {
    uint32_t xid;                 // 事务ID
    TransactionState state;      // 状态：进行中、已提交、已中止
    uint64_t start_time;         // 开始时间
    uint32_t snapshot;           // 快照
    uint32_t lsn;                // 日志序列号
    bool holding_exclusive_lock; // 是否持有排他锁
    bool holding_shared_lock;    // 是否持有共享锁
} Transaction;
// === 模拟 PGPROC ===
typedef struct PGPROC {
    Transaction *txn;       // 指向事务对象
    sem_t sema;             // 用于等待锁的阻塞
    struct PGPROC *next;    // 等待链表
    bool waiting_for_excl;  // 是否等待排他锁
} PGPROC;

// === 模拟 proclist_head ===
typedef struct proclist_head {
    PGPROC *head;
    PGPROC *tail;
} proclist_head;

// === 模拟 LWLock ===
typedef struct LWLock {
    uint16_t tranche;
    atomic_uint state;        // bit0 = exclusive，bit1~ = shared count
    proclist_head waiters;
} LWLock;

typedef struct {
    char table_name[64];
    uint32_t oid;  // 该行的 OID（Tuple 中的 oid 字段）
} RowLockTag;
typedef struct RowLock {
    RowLockTag tag;
    PGPROC *holder;           // 持有锁的事务
    PGPROC *wait_queue_head;  // 阻塞等待队列
    struct RowLock *next;
} RowLock;

typedef struct Page {
    PageHeader header;
    Slot slots[MAX_SLOTS];          // 槽位数组
    uint8_t data[PAGE_DATA_SIZE];   // 数据区
       // ✅ 每页一个锁
    LWLock lock;
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

  // ✅ 新增：元组的最大 OID
    uint32_t max_row_oid;


    LWLock fsm_lock;
    LWLock extension_lock;
} TableMeta;






#define MAX_CONCURRENT_TRANS 10

// 无效事务ID
#define INVALID_XID 0





// 事务管理器
typedef struct {
    Transaction transactions[MAX_CONCURRENT_TRANS]; // 事务数组
    uint32_t next_xid;         // 下一个可用事务ID
    uint32_t oldest_xid;       // 最老活动事务ID
} TransactionManager;

// 系统目录
typedef struct {
    TableMeta tables[MAX_TABLES];
    uint16_t table_count;
    uint32_t next_oid;       // 下一个对象ID
} SystemCatalog;

// 数据库状态
typedef struct {
    SystemCatalog catalog;   // 系统目录
    TransactionManager tx_mgr; // 事务管理器
    char data_dir[256];      // 数据目录
    uint32_t current_xid;    // 当前活动事务ID
    PageID next_page_id; // 用于分配新页面ID

    TableMeta* tables;      // 表元数据数组

int table_count;        // 表数量
char* db_name;          // 数据库名称
char* db_path;          // 数据库路径
} MiniDB;

typedef struct {
    int client_fd;             // 客户端 socket fd
    MiniDB* db;                // 指向数据库
    uint32_t current_xid;      // 当前连接的事务 ID
    
} Session;


#endif // TYPES_H