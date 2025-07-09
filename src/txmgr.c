#include "minidb.h"
#include "txmgr.h"
#include "lock.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include "xid_info.h"
extern XidInfo* xid_info; 
extern int xid_shmid;
LWLock TxMgrLock; // 用于保护事务管理器的全局锁

// 事务初始化前需调用一次
void InitTransactionManagerLock() {
    LWLockInit(&TxMgrLock, 1);
}

// 初始化事务管理器
void txmgr_init(TransactionManager *txmgr) {
   //  memset(txmgr, 0, sizeof(TransactionManager));
   
       
    

}

// 查找空闲事务槽
static int find_free_transaction_slot(TransactionManager *txmgr) {
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        printf("%p\n",txmgr);
        if (txmgr->transactions[i].state == TRANS_NONE || 
            txmgr->transactions[i].xid == INVALID_XID) {
            return i;
        }
    }
    return -1; // 无可用槽位
}

// 开始新事务
uint32_t txmgr_start_transaction(MiniDB *db) {
    // 查找空闲事务槽
    int slot = find_free_transaction_slot(&(db->tx_mgr));
    if (slot < 0) {
        fprintf(stderr, "Error: Maximum concurrent transactions reached (%d)\n", 
                MAX_CONCURRENT_TRANS);
        return INVALID_XID;
    }
    
    // 分配事务ID
    uint32_t xid =db->tx_mgr->next_xid++;
    if (xid == INVALID_XID) {
        xid = db->tx_mgr->next_xid++; // 跳过无效ID
    }
    
    // 初始化事务
    db->tx_mgr->transactions[slot].xid = xid;
    db->tx_mgr->transactions[slot].state = TRANS_ACTIVE;
    db->tx_mgr->transactions[slot].start_time = (uint64_t)time(NULL) * 1000000; // 微秒精度
    db->tx_mgr->transactions[slot].snapshot = db->tx_mgr->oldest_xid;
    db->tx_mgr->transactions[slot].lsn = 0;
    
    // 更新最老事务ID（如果是第一个活动事务）
    if (db->tx_mgr->oldest_xid == 0 || db->tx_mgr->oldest_xid > xid) {
        db->tx_mgr->oldest_xid = xid;
    }
    
    printf("Started transaction %u (slot %d)\n", xid, slot);
    // 记录WAL
    wal_log_begin(xid);
    
    return xid;
}

// 根据XID查找事务
static Transaction *find_transaction(TransactionManager *txmgr, uint32_t xid) {
    if (xid == INVALID_XID) return NULL;
    
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        if (txmgr->transactions[i].xid == xid) {
            return &txmgr->transactions[i];
        }
    }
    return NULL;
}


void txmgr_commit_transaction(MiniDB *db, uint32_t xid) {
    Transaction *trans = find_transaction(&(db->tx_mgr), xid);
    if (!trans) {
        fprintf(stderr, "Error: Commit failed - transaction %u not found\n", xid);
        return;
    }

    if (trans->state != TRANS_ACTIVE) {
        fprintf(stderr, "Error: Commit failed - transaction %u is not active (state: %d)\n", 
                xid, trans->state);
        return;
    }

    // 设置为已提交状态
    trans->state = TRANS_COMMITTED;

    // 设置提交位图
    pthread_mutex_lock(&db->tx_mgr->lock);
    SET_COMMITTED(xid, db->tx_mgr);
    pthread_mutex_unlock(&db->tx_mgr->lock);
    printf("SET_COMMITTED: xid=%u, byte=%d, bit=%d\n", xid, xid/8, xid%8);
    printf("txmgr->committed_bitmap[%d] = %02x\n", xid / 8, db->tx_mgr->committed_bitmap[xid / 8]);

    // 记录WAL
    wal_log_commit(xid);

    printf("Committed transaction %u\n", xid);

    // 更新最老事务 ID
    if (xid == db->tx_mgr->oldest_xid) {
        uint32_t new_oldest = UINT32_MAX;
        for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
            if (db->tx_mgr->transactions[i].state == TRANS_ACTIVE &&
                db->tx_mgr->transactions[i].xid < new_oldest) {
                new_oldest = db->tx_mgr->transactions[i].xid;
            }
        }
        db->tx_mgr->oldest_xid = (new_oldest == UINT32_MAX)
            ? db->tx_mgr->next_xid
            : new_oldest;

        printf("Updated oldest XID to %u\n", db->tx_mgr->oldest_xid);
    }

    // 💡 移除事务槽，释放资源（更安全）
    trans->xid = 0;
    trans->state = TRANS_NONE;

    save_tx_state(db->tx_mgr, db->data_dir);  // ✅ 保存 bitmap
}
void tcp_txmgr_commit_transaction(MiniDB *db, uint32_t xid) {
    Transaction *trans = find_transaction(db->tx_mgr, xid);
    if (!trans) {
        fprintf(stderr, "Error: Commit failed - transaction %u not found\n", xid);
        return;
    }

    if (trans->state != TRANS_ACTIVE) {
        fprintf(stderr, "Error: Commit failed - transaction %u is not active (state: %d)\n", 
                xid, trans->state);
        return;
    }

    // 设置为已提交状态
    trans->state = TRANS_COMMITTED;

    // 设置提交位图
        pthread_mutex_lock(&db->tx_mgr->lock);
    SET_COMMITTED(xid, db->tx_mgr);
       pthread_mutex_unlock(&db->tx_mgr->lock);
    printf("SET_COMMITTED: xid=%u, byte=%d, bit=%d\n", xid, xid/8, xid%8);
    printf("txmgr->committed_bitmap[%d] = %02x\n", xid / 8, db->tx_mgr->committed_bitmap[xid / 8]);

    // 记录WAL
    wal_log_commit(xid);

    printf("Committed transaction %u\n", xid);

    // 更新最老事务 ID
    if (xid == db->tx_mgr->oldest_xid) {
        uint32_t new_oldest = UINT32_MAX;
        for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
           //   printf("[txslot %d] xid = %u, state = %d\n",i, db->tx_mgr->transactions[i].xid,db->tx_mgr->transactions[i].state);
            if (db->tx_mgr->transactions[i].state == TRANS_ACTIVE &&
                db->tx_mgr->transactions[i].xid < new_oldest) {
                new_oldest = db->tx_mgr->transactions[i].xid;
            }
        }
        //db->tx_mgr->oldest_xid = (new_oldest == UINT32_MAX)? db->tx_mgr->next_xid: new_oldest;
         db->tx_mgr->oldest_xid = (new_oldest == UINT32_MAX)? xid_info->next_xid: new_oldest;
        printf("Updated oldest XID to %u\n", db->tx_mgr->oldest_xid);
    }

    // 💡 移除事务槽，释放资源（更安全）
    trans->xid = 0;
    trans->state = TRANS_NONE;

    tcp_save_tx_state(db->tx_mgr, db->data_dir,xid_info->next_xid);  // ✅ 保存 bitmap
}

uint32_t tcp_txmgr_start_transaction(MiniDB *db, Session* session) {
    int slot = find_free_transaction_slot(db->tx_mgr);
    if (slot < 0) {
        fprintf(stderr, "Error: Maximum concurrent transactions reached (%d)\n", 
                MAX_CONCURRENT_TRANS);
        return INVALID_XID;
    }

    // 从共享 xid 分配器中获取
    uint32_t xid = allocate_global_xid();  // ✅ 替换原来的 db->tx_mgr->next_xid++

    db->tx_mgr->transactions[slot].xid = xid;
    db->tx_mgr->transactions[slot].state = TRANS_ACTIVE;
    db->tx_mgr->transactions[slot].start_time = (uint64_t)time(NULL) * 1000000;
    db->tx_mgr->transactions[slot].snapshot = db->tx_mgr->oldest_xid;
    db->tx_mgr->transactions[slot].lsn = 0;

    if (db->tx_mgr->oldest_xid == 0 || db->tx_mgr->oldest_xid > xid) {
        db->tx_mgr->oldest_xid = xid;
    }

    printf("tcp Started transaction %u (slot %d)\n", xid, slot);
    wal_log_begin(xid);

    // 把 xid 回传给 session
    session->current_xid = xid;

    return xid;
}
void tcp_txmgr_abort_transaction(MiniDB * db, uint32_t xid) {
    Transaction *trans = find_transaction(db->tx_mgr, xid);
    if (!trans) {
        fprintf(stderr, "Error: Abort failed - transaction %u not found\n", xid);
        return;
    }
    
    if (trans->state != TRANS_ACTIVE) {
        fprintf(stderr, "Error: Abort failed - transaction %u is not active (state: %d)\n", 
                xid, trans->state);
        return;
    }
    
    // 更新事务状态
    trans->state = TRANS_ABORTED;
     //  db->tx_mgr->committed_flags[xid] = 3;  // 内存中标记
     CLEAR_COMMITTED(xid, db->tx_mgr);
    // 记录WAL
    wal_log_abort(xid);

    printf("Aborted transaction %u\n", xid);
    
    // 更新最老事务ID（如果需要）
    if (xid == db->tx_mgr->oldest_xid) {
        uint32_t new_oldest = UINT32_MAX;
        
        // 查找最小活动事务ID
        for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
            if (db->tx_mgr->transactions[i].state == TRANS_ACTIVE && 
               db->tx_mgr->transactions[i].xid < new_oldest) {
                new_oldest =db->tx_mgr->transactions[i].xid;
            }
        }
        
        // 如果没有活动事务，重置为下一个可用XID
        if (new_oldest == UINT32_MAX) {
            db->tx_mgr->oldest_xid = db->tx_mgr->next_xid;
        } else {
            db->tx_mgr->oldest_xid = new_oldest;
        }
        tcp_save_tx_state(db->tx_mgr, db->data_dir,xid_info->next_xid);  // ✅ 保存事务状态
        printf("Updated oldest XID to %u\n",db->tx_mgr->oldest_xid);
    }
}


// 中止事务
void txmgr_abort_transaction(MiniDB * db, uint32_t xid) {
    Transaction *trans = find_transaction(db->tx_mgr, xid);
    if (!trans) {
        fprintf(stderr, "Error: Abort failed - transaction %u not found\n", xid);
        return;
    }
    
    if (trans->state != TRANS_ACTIVE) {
        fprintf(stderr, "Error: Abort failed - transaction %u is not active (state: %d)\n", 
                xid, trans->state);
        return;
    }
    
    // 更新事务状态
    trans->state = TRANS_ABORTED;
     //  db->tx_mgr->committed_flags[xid] = 3;  // 内存中标记
     CLEAR_COMMITTED(xid, db->tx_mgr);
    // 记录WAL
    wal_log_abort(xid);

    printf("Aborted transaction %u\n", xid);
    
    // 更新最老事务ID（如果需要）
    if (xid == db->tx_mgr->oldest_xid) {
        uint32_t new_oldest = UINT32_MAX;
        
        // 查找最小活动事务ID
        for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
            if (db->tx_mgr->transactions[i].state == TRANS_ACTIVE && 
               db->tx_mgr->transactions[i].xid < new_oldest) {
                new_oldest =db->tx_mgr->transactions[i].xid;
            }
        }
        
        // 如果没有活动事务，重置为下一个可用XID
        if (new_oldest == UINT32_MAX) {
            db->tx_mgr->oldest_xid = db->tx_mgr->next_xid;
        } else {
            db->tx_mgr->oldest_xid = new_oldest;
        }
        save_tx_state(&db->tx_mgr, db->data_dir);  // ✅ 保存事务状态
        printf("Updated oldest XID to %u\n",db->tx_mgr->oldest_xid);
    }
}

// 检查元组对当前事务是否可见
int txmgr_is_visible(const TransactionManager *txmgr, uint32_t xid, 
                     uint32_t tuple_xmin, uint32_t tuple_xmax) {
    // 查找当前事务
    const Transaction *current = NULL;
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        if (txmgr->transactions[i].xid == xid) {
            current = &txmgr->transactions[i];
            break;
        }
    }
    
    if (!current) {
        fprintf(stderr, "Error: Visibility check failed - transaction %u not found\n", xid);
        return 0;
    }
    
    if (current->state != TRANS_ACTIVE) {
        fprintf(stderr, "Error: Visibility check failed - transaction %u is not active\n", xid);
        return 0;
    }
    
    // 规则1: 元组由当前事务创建
    if (tuple_xmin == xid) {
        // 检查元组是否已被当前事务删除
        if (tuple_xmax == xid || tuple_xmax == INVALID_XID) {
            return 1; // 可见
        }
        return 0; // 不可见（已被当前事务删除）
    }
    
    // 规则2: 元组由已提交事务创建
    if (tuple_xmin != INVALID_XID && tuple_xmin < current->snapshot) {
        // 检查元组是否已被删除
        if (tuple_xmax == INVALID_XID) {
            return 1; // 未被删除，可见
        }
        
        // 如果删除事务尚未提交，或删除事务在当前事务之后开始，则可见
        if (tuple_xmax > current->xid) {
            return 1; // 删除事务在当前事务之后，可见
        }
        
        // 检查删除事务状态
        if (tuple_xmax != INVALID_XID) {
            const Transaction *delete_trans = find_transaction((TransactionManager *)txmgr, tuple_xmax);
            if (!delete_trans) {
                // 事务可能已提交或不存在
                return 0; // 安全起见不可见
            }
            
            if (delete_trans->state != TRANS_COMMITTED) {
                return 1; // 删除事务未提交，可见
            }
        }
    }
    
    // 规则3: 元组由活动事务创建（但非当前事务）
    // 在快照隔离中，这些元组不可见
    return 0;
}

// 获取事务状态
TransactionState txmgr_get_transaction_state(TransactionManager *txmgr, uint32_t xid) {
    Transaction *trans = find_transaction(txmgr, xid);
    return trans ? trans->state : TRANS_ABORTED;
}

// 打印事务状态
void txmgr_print_status(TransactionManager *txmgr) {
    printf("\nTransaction Manager Status:\n");
    printf("Next XID: %u\n", txmgr->next_xid);
    printf("Oldest XID: %u\n", txmgr->oldest_xid);
    
    printf("\nActive Transactions list:\n");
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        if (txmgr->transactions[i].state == TRANS_ACTIVE) {
            time_t start_sec = txmgr->transactions[i].start_time / 1000000;
            struct tm *tm_info = localtime(&start_sec);
            char time_buf[20];
            strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
            
            printf("  XID: %-6u Start: %s.%03u Snapshot: %u\n",
                   txmgr->transactions[i].xid,
                   time_buf,
                   (unsigned)(txmgr->transactions[i].start_time % 1000000) / 1000,
                   txmgr->transactions[i].snapshot);
        }
    }
    
    printf("\n");
}
bool load_tx_state(TransactionManager* txmgr, const char* db_path) {
    char path[256];
    snprintf(path, sizeof(path), "%s/tx_state.tx", db_path);

    FILE* fp = fopen(path, "rb");
    if (!fp) {
        memset(txmgr, 0, sizeof(TransactionManager));
        // 如果没有文件，初始化为默认值
        txmgr->next_xid = 1;
        txmgr->oldest_xid = 1;

        for (int i = 0; i < MAX_CONCURRENT_TRANS; ++i) {
            txmgr->transactions[i].xid = 0;
            txmgr->transactions[i].state = TRANS_NONE;
              
        }
            // 初始化 committed_flags
    memset(txmgr->committed_bitmap, 0, sizeof(txmgr->committed_bitmap));
        return false;
    }

    fread(&txmgr->next_xid, sizeof(uint32_t), 1, fp);
    fread(&txmgr->oldest_xid, sizeof(uint32_t), 1, fp);
     // 读取事务状态槽
    //fread(txmgr->transactions, sizeof(Transaction), MAX_CONCURRENT_TRANS, fp);
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
    fread(&txmgr->transactions[i].xid, sizeof(uint32_t), 1, fp);
    fread(&txmgr->transactions[i].state, sizeof(TransactionState), 1, fp);
}

    //fwrite(txmgr->committed_flags, sizeof(uint8_t), MAX_XID, fp);
     fread(txmgr->committed_bitmap, 1, COMMIT_BITMAP_SIZE, fp);

    fclose(fp);
    printf("Transaction manager initialized. Next XID: %u, Oldest XID: %u,txmgr->committed_bitmap[2]:%d\n", 
           txmgr->next_xid, txmgr->oldest_xid,txmgr->committed_bitmap[2]);
    return true;
}

bool tcp_load_tx_state(TransactionManager* txmgr, const char* db_path) {
    char path[256];
    snprintf(path, sizeof(path), "%s/tcp_tx_state.tx", db_path);

    FILE* fp = fopen(path, "rb");
    if (!fp) {
        memset(txmgr, 0, sizeof(TransactionManager));
        // 如果没有文件，初始化为默认值
        txmgr->next_xid = 1;
        txmgr->oldest_xid = 1;

        for (int i = 0; i < MAX_CONCURRENT_TRANS; ++i) {
            txmgr->transactions[i].xid = 0;
            txmgr->transactions[i].state = TRANS_NONE;
              
        }
            // 初始化 committed_flags
    memset(txmgr->committed_bitmap, 0, sizeof(txmgr->committed_bitmap));
        return false;
    }

    //fread(&txmgr->next_xid, sizeof(uint32_t), 1, fp);

    uint32_t persisted_next_xid = 0;
    fread(&persisted_next_xid, sizeof(uint32_t), 1, fp);
    xid_info->next_xid = persisted_next_xid;  // ✅ 写入共享内存
    txmgr->next_xid=xid_info->next_xid ;  //Jimmy add 同步  txmgr->next_xid
    fread(&txmgr->oldest_xid, sizeof(uint32_t), 1, fp);
     // 读取事务状态槽
    //fread(txmgr->transactions, sizeof(Transaction), MAX_CONCURRENT_TRANS, fp);
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
    fread(&txmgr->transactions[i].xid, sizeof(uint32_t), 1, fp);
    fread(&txmgr->transactions[i].state, sizeof(TransactionState), 1, fp);
}

    //fwrite(txmgr->committed_flags, sizeof(uint8_t), MAX_XID, fp);
     fread(txmgr->committed_bitmap, 1, COMMIT_BITMAP_SIZE, fp);

    fclose(fp);
    printf("Transaction manager initialized. Next XID: %u, Oldest XID: %u,txmgr->committed_bitmap[2]:%d\n", txmgr->next_xid, txmgr->oldest_xid,txmgr->committed_bitmap[2]);
    return true;
}
bool save_tx_state(const TransactionManager* txmgr, const char* db_path) {
    char path[256];
    snprintf(path, sizeof(path), "%s/tx_state.tx", db_path);

    FILE* fp = fopen(path, "wb");
    if (!fp) {
        perror("save_tx_state: fopen failed");
        return false;
    }

    fwrite(&txmgr->next_xid, sizeof(uint32_t), 1, fp);
    fwrite(&txmgr->oldest_xid, sizeof(uint32_t), 1, fp);
    
   
     // 写入入当前事务槽的活跃状态（调试用）
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        fwrite(&txmgr->transactions[i].xid, sizeof(uint32_t), 1, fp);
        fwrite(&txmgr->transactions[i].state, sizeof(TransactionState), 1, fp);
    }
     // 写入 committed_flags 状态（假设 MAX_XID 定义合理）
    //fwrite(txmgr->committed_flags, sizeof(uint8_t),MAX_XID, fp);
     fwrite(txmgr->committed_bitmap, 1, COMMIT_BITMAP_SIZE, fp);  // 写bitmap
    fflush(fp);
    fclose(fp);
    return true;
}

  bool tcp_save_tx_state(const TransactionManager* txmgr, const char* db_path, uint32_t external_next_xid) {
    if (!txmgr || !db_path) return false;

    char path[256];
    snprintf(path, sizeof(path), "%s/tcp_tx_state.tx", db_path);

    FILE* fp = fopen(path, "wb");
    if (!fp) {
        perror("tcp save_tx_state: fopen failed");
        return false;
    }

    // ✅ 保存的是共享内存中的 next_xid，而不是 txmgr->next_xid
    fwrite(&external_next_xid, sizeof(uint32_t), 1, fp);

    // ✅ 其余数据来自当前进程维护的 txmgr
    fwrite(&txmgr->oldest_xid, sizeof(uint32_t), 1, fp);

    // ✅ 保存事务槽信息（调试用，也可以用于恢复状态）
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        fwrite(&txmgr->transactions[i].xid, sizeof(uint32_t), 1, fp);
        fwrite(&txmgr->transactions[i].state, sizeof(TransactionState), 1, fp);
    }

    // ✅ 保存提交状态 bitmap
    fwrite(txmgr->committed_bitmap, 1, COMMIT_BITMAP_SIZE, fp);

    fflush(fp);
    fclose(fp);
    return true;
}



bool new_old_txmgr_is_committed(const TransactionManager* txmgr,uint32_t xid) {
    for (int i = 0; i < txmgr->next_xid; ++i) {
        if (txmgr->transactions[i].xid == xid) {
            return txmgr->transactions[i].state == TRANS_COMMITTED;
        }
    }
    return false;  // xid 不存在视为未提交
}
bool old_txmgr_is_committed(const TransactionManager* txmgr, uint32_t xid) {
    if (xid >= MAX_XID || xid == INVALID_XID) return false;
      return IS_COMMITTED(xid, txmgr);
}
bool txmgr_is_committed(const TransactionManager* txmgr, uint32_t xid) {
    uint32_t byte_idx = xid / 8;
    uint8_t bit_mask = 1 << (xid % 8);
    bool committed = (txmgr->committed_bitmap[byte_idx] & bit_mask) != 0;
    
    printf("[txmgr_is_committed] xid=%u, committed_bitmap[0]=0x%x, result=%d (txmgr=%p)\n", 
            xid, txmgr->committed_bitmap[0], committed, txmgr);
    
    return committed;
}

uint32_t old_compute_snapshot_xmin(TransactionManager* txmgr) {
    uint32_t xmin = txmgr->next_xid;
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        if (txmgr->transactions[i].state == TRANS_ACTIVE &&
            txmgr->transactions[i].xid < xmin) {
            xmin = txmgr->transactions[i].xid;
        }
    }
    printf("compute_snapshot_xmin :%d\n",xmin);
    return xmin;
}
void compute_snapshot(TransactionManager *txmgr, uint32_t current_xid, Snapshot *snap) {
   // snap->xmin = txmgr->next_xid;
    //snap->xmax = txmgr->next_xid;
     snap->xmax = xid_info->next_xid;   // ✅ 当前分配器的最大 XID
    snap->xmin =current_xid;   // ✅ 初始设为 xmax，等待找出最小活跃 xid
    snap->active_count = 0;

    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        uint32_t xid = txmgr->transactions[i].xid;
    

        if (xid == 0 || xid == current_xid) continue;
        // 只保留未提交事务为活跃事务
        if (!txmgr_is_committed(txmgr, xid)) {
            snap->active_xids[snap->active_count++] = xid;

            if (xid < snap->xmin) {
                snap->xmin = xid;
            }
        }

    }

     // 如果没有其他活跃事务，xmin = current_xid
    if (snap->active_count == 0) {
        snap->xmin = current_xid;
    }

    printf("[compute_snapshot] snapshot: xmin=%u, xmax=%u, active_xids = [", snap->xmin, snap->xmax);
    for (int i = 0; i < snap->active_count; i++) {
        printf("%u,", snap->active_xids[i]);
    }
    printf("]");
}