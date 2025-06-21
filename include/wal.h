#ifndef WAL_H
#define WAL_H

#include <stdint.h>
#include <zlib.h>

#include "minidb.h"
// WAL文件名
#define WAL_FILE "pg_wal.log"

// WAL记录类型
typedef enum {
    WAL_BEGIN = 0x00,         // 事务开始
    WAL_INSERT = 0x01,        // 插入记录
    WAL_UPDATE = 0x02,        // 更新记录
    WAL_DELETE = 0x03,        // 删除记录
    WAL_CREATE_TABLE = 0x10,  // 创建表
    WAL_COMMIT = 0x20,        // 事务提交
    WAL_ABORT = 0x21,         // 事务中止
    WAL_CHECKPOINT = 0x30     // 检查点
} WalRecordType;

// WAL记录头
typedef struct {
    uint32_t crc;           // CRC32校验码
    uint16_t total_len;     // 记录总长度（包括头）
    WalRecordType type;     // 记录类型
    uint32_t lsn;           // 日志序列号
    uint32_t xid;           // 事务ID
    uint64_t timestamp;     // 时间戳（微秒精度）
} WalRecordHeader;

// WAL插入记录
typedef struct {
    uint32_t table_oid;     // 表OID
    uint16_t tuple_len;     // 元组长度
    // 后面跟着序列化的元组数据
} WalInsertRecord;

// WAL创建表记录
typedef struct {
    uint32_t table_oid;     // 表OID
    char table_name[32];    // 表名
    uint8_t col_count;      // 列数
    // 后面跟着列定义数组
} WalCreateTableRecord;

/**
 * @brief 初始化WAL系统
 */
void init_wal();

/**
 * @brief 记录事务开始
 * @param xid 事务ID
 */
void wal_log_begin(uint32_t xid);

/**
 * @brief 记录事务提交
 * @param xid 事务ID
 */
void wal_log_commit(uint32_t xid);

/**
 * @brief 记录事务中止
 * @param xid 事务ID
 */
void wal_log_abort(uint32_t xid);

/**
 * @brief 记录插入操作
 * @param xid 事务ID
 * @param table_oid 表OID
 * @param tuple 插入的元组
 */
void wal_log_insert(uint32_t xid, uint32_t table_oid, const Tuple *tuple);

/**
 * @brief 记录创建表操作
 * @param meta 表元数据
 * @param xid 事务ID
 */
void wal_log_create_table(const TableMeta *meta, uint32_t xid);

/**
 * @brief 创建检查点
 */
void wal_log_checkpoint();

/**
 * @brief 从WAL恢复数据库
 * @param db 数据库实例
 */
//void recover_from_wal(MiniDB *db);

#endif // WAL_H