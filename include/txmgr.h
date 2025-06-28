#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include <stdint.h>
#include <time.h>
#include "types.h"
#define SET_COMMITTED(xid, txmgr)   ((txmgr)->committed_bitmap[(xid)/8] |=  (1 << ((xid)%8)))
#define IS_COMMITTED(xid, txmgr)    ((txmgr)->committed_bitmap[(xid)/8] &   (1 << ((xid)%8)))
#define CLEAR_COMMITTED(xid, txmgr) ((txmgr)->committed_bitmap[(xid)/8] &= ~(1 << ((xid)%8)))





/**
 * @brief 初始化事务管理器
 * 
 * @param txmgr 事务管理器指针
 */
void txmgr_init(TransactionManager *txmgr);

/**
 * @brief 开始新事务
 * 
 * @param txmgr 事务管理器指针
 * @return uint32_t 新事务ID（INVALID_XID表示失败）
 */
//uint32_t txmgr_start_transaction(TransactionManager *txmgr);
uint32_t txmgr_start_transaction(MiniDB *db);
/**
 * @brief 提交事务
 * 
 * @param txmgr 事务管理器指针
 * @param xid 要提交的事务ID
 */
void txmgr_commit_transaction(MiniDB * db, uint32_t xid);

/**
 * @brief 中止事务
 * 
 * @param txmgr 事务管理器指针
 * @param xid 要中止的事务ID
 */
void txmgr_abort_transaction(MiniDB * db, uint32_t xid);

/**
 * @brief 检查元组对指定事务是否可见
 * 
 * @param txmgr 事务管理器指针
 * @param xid 事务ID
 * @param tuple_xmin 元组创建事务ID
 * @param tuple_xmax 元组删除事务ID
 * @return int 1可见，0不可见
 */
int txmgr_is_visible(const TransactionManager *txmgr, uint32_t xid, 
                     uint32_t tuple_xmin, uint32_t tuple_xmax);

/**
 * @brief 获取事务状态
 * 
 * @param txmgr 事务管理器指针
 * @param xid 事务ID
 * @return TransactionState 事务状态
 */
TransactionState txmgr_get_transaction_state(TransactionManager *txmgr, uint32_t xid);

/**
 * @brief 打印事务管理器状态
 * 
 * @param txmgr 事务管理器指针
 */
void txmgr_print_status(TransactionManager *txmgr);

/**
 * @brief 获取最老活动事务ID
 * 
 * @param txmgr 事务管理器指针
 * @return uint32_t 最老活动事务ID
 */
uint32_t txmgr_get_oldest_xid(const TransactionManager *txmgr);

/**
 * @brief 获取下一个可用事务ID
 * 
 * @param txmgr 事务管理器指针
 * @return uint32_t 下一个可用事务ID
 */
uint32_t txmgr_get_next_xid(const TransactionManager *txmgr);

bool txmgr_is_committed(const TransactionManager* txmgr,uint32_t xid);

bool save_tx_state(const TransactionManager* txmgr, const char* db_path) ;

bool load_tx_state(TransactionManager* txmgr, const char* db_path);
#endif // TRANSACTION_MANAGER_H