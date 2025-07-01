#include "wal.h"
#include "minidb.h"
#include "txmgr.h"
#include "catalog.h"
#include "page.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <zlib.h>

// 当前LSN（日志序列号）
static uint32_t current_lsn = 0;

// 全局 PageCache 外部声明
extern PageCache global_page_cache;

// ---------------- CRC 计算 ------------------

static uint32_t calculate_crc32(const void *data, size_t length) {
    return crc32(0, data, length);
}

// ---------------- WAL 记录写入 ------------------

static uint32_t write_wal_record(WalRecordType type, uint32_t xid, const void *data, size_t data_len) {
    FILE *wal_file = fopen(WAL_FILE, "r+b");
    if (!wal_file) {
        // 如果文件不存在，说明是第一次，创建它
        wal_file = fopen(WAL_FILE, "w+b");
    }
    fseek(wal_file, 0, SEEK_END);  // 这一步很关键
    // 记录当前偏移位置作为 LSN
    long lsn_offset = ftell(wal_file);
      printf("get LSN offset:%ld\n",lsn_offset);
    if (lsn_offset == -1) {
        perror("Failed to get LSN offset");
        fclose(wal_file);
        return;
    }


    WalRecordHeader header;
    header.type = type;
    header.lsn=lsn_offset;
    //header.lsn = current_lsn++;
    header.xid = xid;
    header.timestamp = (uint64_t)time(NULL) * 1000000;
    header.total_len = sizeof(WalRecordHeader) + data_len;
    header.crc = 0;

    long record_start = ftell(wal_file);

    fwrite(&header, sizeof(header), 1, wal_file);
    if (data_len > 0) fwrite(data, data_len, 1, wal_file);
    fflush(wal_file);

    // CRC 修正
    fseek(wal_file, record_start, SEEK_SET);
    uint8_t *buf = malloc(header.total_len);
    fread(buf, header.total_len, 1, wal_file);
    uint32_t crc = calculate_crc32(buf + sizeof(uint32_t), header.total_len - sizeof(uint32_t));
    fseek(wal_file, record_start, SEEK_SET);
    fwrite(&crc, sizeof(crc), 1, wal_file);
    fseek(wal_file, 0, SEEK_END);
    free(buf);

    fclose(wal_file);
    return header.lsn;
}

// ---------------- 公共接口 ------------------

void init_wal() {
    FILE *f = fopen(WAL_FILE, "ab");
    if (f) fclose(f);
}

void wal_log_begin(uint32_t xid) {
    write_wal_record(WAL_BEGIN, xid, NULL, 0);
}

void wal_log_commit(uint32_t xid) {
    write_wal_record(WAL_COMMIT, xid, NULL, 0);
}

void wal_log_abort(uint32_t xid) {
    write_wal_record(WAL_ABORT, xid, NULL, 0);
}

void wal_log_insert(uint32_t xid, uint32_t table_oid, const Tuple *tuple) {
    uint8_t tuple_buf[PAGE_SIZE];
    int tuple_len = serialize_tuple(tuple, tuple_buf);

    WalInsertRecord rec;
    rec.table_oid = table_oid;
    rec.tuple_len = tuple_len;

    size_t total = sizeof(WalInsertRecord) + tuple_len;
    uint8_t *buffer = malloc(total);
    memcpy(buffer, &rec, sizeof(rec));
    memcpy(buffer + sizeof(rec), tuple_buf, tuple_len);

    write_wal_record(WAL_INSERT, xid, buffer, total);
    free(buffer);
}

void wal_log_create_table(const TableMeta *meta, uint32_t xid) {
    WalCreateTableRecord rec;
    rec.table_oid = meta->oid;
    strncpy(rec.table_name, meta->name, sizeof(rec.table_name));
    rec.col_count = meta->col_count;

    size_t total = sizeof(WalCreateTableRecord) + sizeof(ColumnDef) * meta->col_count;
    uint8_t *buffer = malloc(total);
    memcpy(buffer, &rec, sizeof(rec));
    memcpy(buffer + sizeof(rec), meta->cols, sizeof(ColumnDef) * meta->col_count);

    write_wal_record(WAL_CREATE_TABLE, xid, buffer, total);
    free(buffer);
}



// ---------------- 创建检查点 ------------------

void wal_log_checkpoint(MiniDB *db) {
    flush_all_dirty_pages(db);
    current_lsn=write_wal_record(WAL_CHECKPOINT, 0, NULL, 0);
    printf("Checkpoint created at LSN %ld\n", current_lsn);
}
