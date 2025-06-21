#include "wal.h"
#include "minidb.h"
#include "txmgr.h"
#include "catalog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <zlib.h>

// 当前LSN（日志序列号）
static uint32_t current_lsn = 0;

// 计算CRC32校验码
static uint32_t calculate_crc32(const void *data, size_t length) {
    return crc32(0, data, length);
}

// 写入WAL记录
static void write_wal_record(WalRecordType type, uint32_t xid, const void *data, size_t data_len) {
    // 打开WAL文件（追加模式）
    FILE *wal_file = fopen(WAL_FILE, "a+b");
    if (!wal_file) {
        perror("Failed to open WAL file");
        return;
    }
    
    // 准备记录头
    WalRecordHeader header;
    header.type = type;
    header.lsn = current_lsn++;
    header.xid = xid;
    header.timestamp = (uint64_t)time(NULL) * 1000000; // 微秒精度
    
    // 计算总长度（头 + 数据）
    header.total_len = sizeof(WalRecordHeader) + data_len;
    header.crc = 0; // 临时设置为0，稍后计算
    
    // 写入记录头（不含CRC）
    if (fwrite(&header, sizeof(WalRecordHeader), 1, wal_file) != 1) {
        perror("Failed to write WAL header");
        fclose(wal_file);
        return;
    }
    
    // 写入数据
    if (data_len > 0 && fwrite(data, data_len, 1, wal_file) != 1) {
        perror("Failed to write WAL data");
        fclose(wal_file);
        return;
    }
    
    // 计算并更新CRC
    fflush(wal_file); // 确保数据写入
    long record_start = ftell(wal_file) - header.total_len;
    fseek(wal_file, record_start, SEEK_SET);
    
    // 读取整个记录计算CRC
    uint8_t *record_buf = malloc(header.total_len);
    if (!record_buf) {
        perror("Failed to allocate memory for CRC calculation");
        fclose(wal_file);
        return;
    }
    
    if (fread(record_buf, header.total_len, 1, wal_file) != 1) {
        perror("Failed to read record for CRC calculation");
        free(record_buf);
        fclose(wal_file);
        return;
    }
    
    // 计算CRC（跳过CRC字段本身）
    uint32_t crc = calculate_crc32(record_buf + sizeof(uint32_t), 
                         header.total_len - sizeof(uint32_t));
    
    // 更新CRC字段
    fseek(wal_file, record_start, SEEK_SET);
    if (fwrite(&crc, sizeof(crc), 1, wal_file) != 1) {
        perror("Failed to update CRC in WAL record");
    }
    
    // 回到文件末尾
    fseek(wal_file, 0, SEEK_END);
    
    free(record_buf);
    fclose(wal_file);
}

// 初始化WAL
void init_wal() {
    // 打开文件确保存在
    FILE *f = fopen(WAL_FILE, "ab");
    if (f) fclose(f);
}

// 记录事务开始
void wal_log_begin(uint32_t xid) {
    write_wal_record(WAL_BEGIN, xid, NULL, 0);
}

// 记录事务提交
void wal_log_commit(uint32_t xid) {  // 添加参数
    write_wal_record(WAL_COMMIT, xid, NULL, 0);
}

// 记录事务中止
void wal_log_abort(uint32_t xid) {
    write_wal_record(WAL_ABORT, xid, NULL, 0);
}

// 记录插入操作
void wal_log_insert(uint32_t xid, uint32_t table_oid, const Tuple *tuple) {
    // 序列化元组
    uint8_t tuple_buffer[PAGE_SIZE];
    int tuple_len = serialize_tuple(tuple, tuple_buffer);
    
    // 准备插入记录
    WalInsertRecord record;
    record.table_oid = table_oid;
    record.tuple_len = tuple_len;
    
    // 计算总数据长度
    size_t total_len = sizeof(WalInsertRecord) + tuple_len;
    uint8_t *buffer = malloc(total_len);
    if (!buffer) {
        perror("Failed to allocate WAL buffer");
        return;
    }
    
    // 复制数据
    memcpy(buffer, &record, sizeof(WalInsertRecord));
    memcpy(buffer + sizeof(WalInsertRecord), tuple_buffer, tuple_len);
    
    // 写入WAL
    write_wal_record(WAL_INSERT, xid, buffer, total_len);
    free(buffer);
}

// 记录创建表操作
void wal_log_create_table(const TableMeta *meta, uint32_t xid) {
    // 准备创建表记录
    WalCreateTableRecord record;
    record.table_oid = meta->oid;
    strncpy(record.table_name, meta->name, sizeof(record.table_name));
    record.col_count = meta->col_count;
    
    // 计算总数据长度
    size_t total_len = sizeof(WalCreateTableRecord) + 
                      sizeof(ColumnDef) * meta->col_count;
    
    uint8_t *buffer = malloc(total_len);
    if (!buffer) {
        perror("Failed to allocate WAL buffer");
        return;
    }
    
    // 复制数据
    memcpy(buffer, &record, sizeof(WalCreateTableRecord));
    memcpy(buffer + sizeof(WalCreateTableRecord), 
           meta->cols, 
           sizeof(ColumnDef) * meta->col_count);
    
    // 写入WAL
    write_wal_record(WAL_CREATE_TABLE, xid, buffer, total_len);
    free(buffer);
}

// 创建检查点
void wal_log_checkpoint() {
    WalRecordHeader header;
    header.type = WAL_CHECKPOINT;
    header.lsn = current_lsn++;
    header.xid = 0; // 检查点没有关联的事务
    header.timestamp = (uint64_t)time(NULL) * 1000000;
    header.total_len = sizeof(WalRecordHeader);
    header.crc = 0;
    
    write_wal_record(WAL_CHECKPOINT, 0, NULL, 0);
    printf("Checkpoint created at LSN %u\n", header.lsn);
}


/*
// 从WAL恢复数据库
void recover_from_wal(MiniDB *db) {
    printf("Starting WAL recovery...\n");
    
    FILE *wal = fopen(WAL_FILE, "rb");
    if (!wal) {
        printf("No WAL found, starting fresh\n");
        return;
    }
    
    WalRecordHeader header;
    uint32_t current_recover_xid = INVALID_XID;
    int in_transaction = 0;
    int recovered_trans = 0;
    int recovered_ops = 0;
    
    // 临时存储表创建信息
    char recover_table_name[MAX_NAME_LEN] = {0};
    ColumnDef recover_columns[MAX_COLS] = {0};
    uint8_t recover_col_count = 0;
    
    while (fread(&header, sizeof(header), 1, wal) == 1) {
        // 计算数据负载长度
        size_t data_len = header.total_len - sizeof(header);
        uint8_t *data = NULL;
        
        if (data_len > 0) {
            data = malloc(data_len);
            if (!data) {
                perror("Failed to allocate memory for WAL data");
                break;
            }
            if (fread(data, data_len, 1, wal) != 1) {
                perror("Failed to read WAL data");
                free(data);
                break;
            }
        }
        
        // 处理记录类型
        switch (header.type) {
            case WAL_BEGIN:
                if (in_transaction) {
                    printf("Warning: Nested transaction not supported, aborting previous\n");
                }
                current_recover_xid = header.xid;
                in_transaction = 1;
                printf("Recovering transaction %u\n", current_recover_xid);
                break;
                
            case WAL_COMMIT:
                if (in_transaction && current_recover_xid == header.xid) {
                    printf("Transaction %u committed\n", current_recover_xid);
                    in_transaction = 0;
                    recovered_trans++;
                } else {
                    printf("Warning: Commit without active transaction\n");
                }
                break;
                
            case WAL_ABORT:
                if (in_transaction && current_recover_xid == header.xid) {
                    printf("Transaction %u aborted\n", current_recover_xid);
                    in_transaction = 0;
                } else {
                    printf("Warning: Abort without active transaction\n");
                }
                break;
                
            case WAL_CREATE_TABLE:
                if (in_transaction && data) {
                    WalCreateTableRecord *rec = (WalCreateTableRecord *)data;
                    strncpy(recover_table_name, rec->table_name, MAX_NAME_LEN);
                    recover_col_count = rec->col_count;
                    
                    if (recover_col_count > 0 && recover_col_count <= MAX_COLS) {
                        memcpy(recover_columns, data + sizeof(WalCreateTableRecord),
                               sizeof(ColumnDef) * recover_col_count);
                        
                        // 重新创建表
                        int oid = create_table(&db->catalog, recover_table_name, 
                                             recover_columns, recover_col_count);
                        if (oid > 0) {
                            printf("Recovered table '%s' (OID: %d)\n", recover_table_name, oid);
                            recovered_ops++;
                        }
                    }
                }
                break;
                
            case WAL_INSERT:
                if (in_transaction && data) {
                    WalInsertRecord *rec = (WalInsertRecord *)data;
                    uint8_t *tuple_data = data + sizeof(WalInsertRecord);
                    
                    // 反序列化元组
                    Tuple tuple;
                    if (deserialize_tuple(&tuple, tuple_data, NULL) > 0) {
                        // 查找表元数据
                        TableMeta *meta = find_table_by_oid(&db->catalog, rec->table_oid);
                        
                        if (meta) {
                            char path[256];
                            snprintf(path, sizeof(path), "%s/%s", db->data_dir, meta->filename);
                            
                            // 将元组写入数据文件
                            DataPage page;
                            if (read_data_page(path, &page) < 0) {
                                init_page_header(&page.header);
                                page.tuple_count = 0;
                            }
                            
                            if (page.tuple_count < 64) {
                                page.tuples[page.tuple_count++] = tuple;
                                if (write_data_page(path, &page) == 0) {
                                    printf("Recovered tuple (OID: %u) for table OID %u\n", 
                                           tuple.oid, rec->table_oid);
                                    recovered_ops++;
                                }
                            }
                        }
                    }
                }
                break;
                
            case WAL_CHECKPOINT:
                printf("Found checkpoint at LSN %u\n", header.lsn);
                // 实际应用中这里会截断WAL
                break;
                
            default:
                printf("Unknown WAL record type: 0x%02X\n", header.type);
                break;
        }
        
        if (data) free(data);
    }
    
    // 处理未完成的事务
    if (in_transaction) {
        printf("Rolling back incomplete transaction %u\n", current_recover_xid);
        // 实际应用中会回滚未提交的更改
    }
    
    fclose(wal);
    printf("WAL recovery completed. Recovered %d transactions and %d operations.\n", 
           recovered_trans, recovered_ops);
}
           */