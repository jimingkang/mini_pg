#include "minidb.h"
#include "txmgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>

// 初始化事务管理器
void txmgr_init(TransactionManager *txmgr) {
    // 初始化事务数组
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        txmgr->transactions[i].xid = INVALID_XID;
        txmgr->transactions[i].state = TRANS_ABORTED;
        txmgr->transactions[i].start_time = 0;
        txmgr->transactions[i].snapshot = 0;
        txmgr->transactions[i].lsn = 0;
    }
    
    // 初始化事务ID计数器
    txmgr->next_xid = 1; // 从1开始，0保留给无效事务
    txmgr->oldest_xid = 1;
    
    printf("Transaction manager initialized. Next XID: %u, Oldest XID: %u\n", 
           txmgr->next_xid, txmgr->oldest_xid);
}

// 查找空闲事务槽
static int find_free_transaction_slot(TransactionManager *txmgr) {
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        if (txmgr->transactions[i].state == TRANS_ABORTED || 
            txmgr->transactions[i].xid == INVALID_XID) {
            return i;
        }
    }
    return -1; // 无可用槽位
}

// 开始新事务
uint32_t txmgr_start_transaction(TransactionManager *txmgr) {
    // 查找空闲事务槽
    int slot = find_free_transaction_slot(txmgr);
    if (slot < 0) {
        fprintf(stderr, "Error: Maximum concurrent transactions reached (%d)\n", 
                MAX_CONCURRENT_TRANS);
        return INVALID_XID;
    }
    
    // 分配事务ID
    uint32_t xid = txmgr->next_xid++;
    if (xid == INVALID_XID) {
        xid = txmgr->next_xid++; // 跳过无效ID
    }
    
    // 初始化事务
    txmgr->transactions[slot].xid = xid;
    txmgr->transactions[slot].state = TRANS_ACTIVE;
    txmgr->transactions[slot].start_time = (uint64_t)time(NULL) * 1000000; // 微秒精度
    txmgr->transactions[slot].snapshot = txmgr->oldest_xid;
    txmgr->transactions[slot].lsn = 0;
    
    // 更新最老事务ID（如果是第一个活动事务）
    if (txmgr->oldest_xid == 0 || txmgr->oldest_xid > xid) {
        txmgr->oldest_xid = xid;
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

// 提交事务
void txmgr_commit_transaction(TransactionManager *txmgr, uint32_t xid) {
    Transaction *trans = find_transaction(txmgr, xid);
    if (!trans) {
        fprintf(stderr, "Error: Commit failed - transaction %u not found\n", xid);
        return;
    }
    
    if (trans->state != TRANS_ACTIVE) {
        fprintf(stderr, "Error: Commit failed - transaction %u is not active (state: %d)\n", 
                xid, trans->state);
        return;
    }
    
    // 更新事务状态
    trans->state = TRANS_COMMITTED;
    
    // 记录WAL
    wal_log_commit(xid);
    
    printf("Committed transaction %u\n", xid);
    
    // 更新最老事务ID（如果需要）
    if (xid == txmgr->oldest_xid) {
        uint32_t new_oldest = UINT32_MAX;
        
        // 查找最小活动事务ID
        for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
            if (txmgr->transactions[i].state == TRANS_ACTIVE && 
                txmgr->transactions[i].xid < new_oldest) {
                new_oldest = txmgr->transactions[i].xid;
            }
        }
        
        // 如果没有活动事务，重置为下一个可用XID
        if (new_oldest == UINT32_MAX) {
            txmgr->oldest_xid = txmgr->next_xid;
        } else {
            txmgr->oldest_xid = new_oldest;
        }
        
        printf("Updated oldest XID to %u\n", txmgr->oldest_xid);
    }
}

// 中止事务
void txmgr_abort_transaction(TransactionManager *txmgr, uint32_t xid) {
    Transaction *trans = find_transaction(txmgr, xid);
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
    
    // 记录WAL
    wal_log_abort(xid);
    
    printf("Aborted transaction %u\n", xid);
    
    // 更新最老事务ID（如果需要）
    if (xid == txmgr->oldest_xid) {
        uint32_t new_oldest = UINT32_MAX;
        
        // 查找最小活动事务ID
        for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
            if (txmgr->transactions[i].state == TRANS_ACTIVE && 
                txmgr->transactions[i].xid < new_oldest) {
                new_oldest = txmgr->transactions[i].xid;
            }
        }
        
        // 如果没有活动事务，重置为下一个可用XID
        if (new_oldest == UINT32_MAX) {
            txmgr->oldest_xid = txmgr->next_xid;
        } else {
            txmgr->oldest_xid = new_oldest;
        }
        
        printf("Updated oldest XID to %u\n", txmgr->oldest_xid);
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