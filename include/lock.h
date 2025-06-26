#ifndef LOCK_H
#define LOCK_H
#include "types.h"

#define ROW_LOCK_BUCKETS 1024

typedef struct {
    RowLock* buckets[ROW_LOCK_BUCKETS];
    LWLock bucket_locks[ROW_LOCK_BUCKETS];  // 每个桶独立锁
} RowLockTable;

RowLockTable global_row_locks;


bool LWLockAcquireExclusive(LWLock *lock) ;

void LWLockInit(LWLock *lock, uint16_t tranche_id);

void init_row_lock_table();
bool lock_row(const char* table, uint32_t oid, uint32_t xid);
void unlock_row(const char* table, uint32_t oid, uint32_t xid);
#endif