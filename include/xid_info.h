#ifndef XID_INFO_H
#define XID_INFO_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "types.h"

#define XID_FILE_PATH "tcp_tx_state.tx"



// 跨进程共享结构体
typedef struct {
    pthread_mutex_t lock;
    uint32_t next_xid;
} XidInfo;

// 全局共享指针
extern XidInfo* xid_info;
extern int xid_shmid;

// 初始化共享内存并加载持久化 xid
bool init_xid_info_shared(MiniDB *global_db ) ;

// 分配全局唯一 xid（线程/进程安全）
uint32_t allocate_global_xid();


// 持久化当前 next_xid
bool persist_next_xid(const char* path);

// 加载持久化 xid（用于启动）
uint32_t load_persisted_xid(const char* path);

// 释放共享内存（主进程退出时）
void cleanup_xid_info();

void init_tx_mgr_shared(MiniDB *global_db) ;
TransactionManager* attach_txmgr_shared(int shmid);

#endif // XID_INFO_H
