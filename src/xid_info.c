#include "xid_info.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>
#include <types.h>

XidInfo* xid_info = NULL;  // 定义全局指针
int xid_shmid = -1;        // 保存共享内存 id，方便释放
TransactionManager* tx_mgr = NULL;
int tx_mgr_shmid = -1;


bool init_xid_info_shared(MiniDB *global_db ) {
    xid_shmid = shmget(IPC_PRIVATE, sizeof(XidInfo), IPC_CREAT | 0666);
    if (xid_shmid < 0) {
        perror("shmget failed");
        return false;
    }

    xid_info = shmat(xid_shmid, NULL, 0);
    if (xid_info == (void*)-1) {
        perror("shmat failed");
        return false;
    }

    // 初始化共享结构
    memset(xid_info, 0, sizeof(XidInfo));


 

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);

    if (pthread_mutex_init(&xid_info->lock, &attr) != 0) {
        perror("pthread_mutex_init failed");
        return false;
    }

        // ✅ 从磁盘恢复事务状态，同时恢复 next_xid
    if (!tcp_load_tx_state(global_db->tx_mgr, global_db->data_dir)) {
        // 若文件不存在则默认 next_xid=1
        xid_info->next_xid = 1;
        printf("[xid_info] No tcp_tx_state.tx found, starting fresh with next_xid = 1\n");
    } else {
        printf("[xid_info] Loaded tcp_tx_state.tx, next_xid = %u\n", xid_info->next_xid);
    }

        tx_mgr->next_xid =  xid_info->next_xid;

    printf("[xid_info] Initialized with next_xid = %u\n", xid_info->next_xid);
    return true;
}




uint32_t allocate_global_xid() {
    if (!xid_info) {
        fprintf(stderr, "allocate_global_xid: xid_info not initialized\n");
        return 0;
    }

    pthread_mutex_lock(&xid_info->lock);
    uint32_t xid = xid_info->next_xid++;
    if (xid == 0) xid = xid_info->next_xid++;  // 跳过无效 xid
    tx_mgr->next_xid=xid;  //Jimmy 同步tx_mgr->next_xid
    pthread_mutex_unlock(&xid_info->lock);
    return xid;
}

void cleanup_xid_info() {
    if (xid_info) {
        shmdt(xid_info);
    }

    if (xid_shmid >= 0) {
        shmctl(xid_shmid, IPC_RMID, NULL);
    }
}


void init_tx_mgr_shared(MiniDB *global_db) {
    size_t size = sizeof(TransactionManager);
    tx_mgr_shmid = shmget(IPC_PRIVATE, size, IPC_CREAT | 0666);
    if (tx_mgr_shmid < 0) {
        perror("shmget tx_mgr failed");
        exit(1);
    }

    tx_mgr = (TransactionManager*) shmat(tx_mgr_shmid, NULL, 0);
    if (tx_mgr == (void*)-1) {
        perror("shmat tx_mgr failed");
        exit(1);
    }
    memset(tx_mgr, 0, size);
     // 初始化事务数组
    for (int i = 0; i < MAX_CONCURRENT_TRANS; i++) {
        tx_mgr->transactions[i].xid = INVALID_XID;
        tx_mgr->transactions[i].state = TRANS_NONE;
        tx_mgr->transactions[i].start_time = 0;
        tx_mgr->transactions[i].snapshot = 0;
        tx_mgr->transactions[i].lsn = 0;
    }
      memset(tx_mgr->committed_bitmap, 0, sizeof(tx_mgr->committed_bitmap));
    // 初始化事务ID计数器
 
        //txmgr->oldest_xid = 1;

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
   //pthread_mutex_init(&tx_mgr->lock, &attr);


   global_db->tx_mgr = tx_mgr;
   printf("[init] tx_mgr = %p\n", tx_mgr);

    printf("[init] TransactionManager shared init done, xid start = %u\n", tx_mgr->next_xid);
}

TransactionManager* attach_txmgr_shared(int shmid) {
    TransactionManager* mgr = (TransactionManager*) shmat(shmid, NULL, 0);
    if (mgr == (void*)-1) {
        perror("attach tx_mgr failed");
        exit(1);
    }
    return mgr;
}
