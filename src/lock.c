
#include "minidb.h"
#include "lock.h"

#define LWLOCK_EXCLUSIVE 0x1
#define LWLOCK_SHARED_MASK 0xFFFE  // 共享锁位
 proclist_init(proclist_head *list) {
    list->head = list->tail = NULL;
}

void proclist_push_back(proclist_head *list, PGPROC *proc) {
    proc->next = NULL;
    if (list->tail) {
        list->tail->next = proc;
    } else {
        list->head = proc;
    }
    list->tail = proc;
}

PGPROC* proclist_pop_front(proclist_head *list) {
    PGPROC *p = list->head;
    if (!p) return NULL;
    list->head = p->next;
    if (!list->head) list->tail = NULL;
    p->next = NULL;
    return p;
}

void LWLockInit(LWLock *lock, uint16_t tranche_id) {
    atomic_store(&lock->state, 0);
    proclist_init(&lock->waiters);
    lock->tranche = tranche_id;
}


bool LWLockAcquireExclusive(LWLock *lock) {
    printf("LWLockAcquireExclusive ");
    uint32_t expected = 0;
    while (!atomic_compare_exchange_weak(&lock->state, &expected, 1)) {
        expected = 0;
        sched_yield(); // or sleep briefly
    }
    return true;
}

void LWLockRelease(LWLock *lock) {
    atomic_store(&lock->state, 0);
}
/*
void LWLockAcquireExclusive(LWLock *lock, PGPROC *myproc) {
    while (true) {
        uint32_t expected = 0;
        if (atomic_compare_exchange_weak(&lock->state, &expected, 1)) {
            // 获取排他锁成功
            return;
        }

        // 加入等待队列
        proclist_push_back(&lock->waiters, myproc);
        sem_wait(&myproc->sema); // 阻塞等待唤醒
    }
}
    void LWLockRelease(LWLock *lock) {
    // 释放锁
    atomic_store(&lock->state, 0);

    // 唤醒一个等待者
    PGPROC *next = proclist_pop_front(&lock->waiters);
    if (next) {
        sem_post(&next->sema);
    }
}
    */

/*
static inline bool atomic_compare_exchange_u32(atomic_uint *obj, uint32_t *expected, uint32_t desired) {
    return atomic_compare_exchange_weak(obj, expected, desired);
}
void LWLockAcquireExclusive(LWLock *lock, PGPROC *myproc) {
    while (true) {
        uint32_t expected = 0;
        if (atomic_compare_exchange_u32(&lock->state, &expected, LWLOCK_EXCLUSIVE)) {
            myproc->txn->holding_exclusive_lock = true;
            return;
        }
        myproc->waiting_for_excl = true;
        proclist_push_back(&lock->waiters, myproc);
        sem_wait(&myproc->sema);
    }
}

void LWLockRelease(LWLock *lock, PGPROC *myproc) {
    uint32_t prev = atomic_load(&lock->state);

    if (myproc->txn->holding_exclusive_lock) {
        atomic_store(&lock->state, 0);
        myproc->txn->holding_exclusive_lock = false;
    } else if (myproc->txn->holding_shared_lock) {
        atomic_fetch_sub(&lock->state, 2);
        myproc->txn->holding_shared_lock = false;
    }

    // 唤醒下一个等待者
    PGPROC *next = proclist_pop_front(&lock->waiters);
    if (next) {
        sem_post(&next->sema);
    }
}

*/


//row lock
uint32_t hash_row_lock_tag(const RowLockTag* tag) {
    // 简单 hash，可替换为更强 hash 算法
    uint32_t h = 0;
    for (int i = 0; tag->table_name[i] && i < 64; i++) {
        h = h * 31 + tag->table_name[i];
    }
    h ^= tag->oid;
    return h;
}

bool row_lock_tag_equal(const RowLockTag* a, const RowLockTag* b) {
    return strcmp(a->table_name, b->table_name) == 0 && a->oid == b->oid;
}
void init_row_lock_table() {
    memset(&global_row_locks, 0, sizeof(global_row_locks));
    for (int i = 0; i < ROW_LOCK_BUCKETS; i++) {
        global_row_locks.bucket_locks[i].state = 0;  // 空锁
    }
}
bool lock_row(const char* table, uint32_t oid, uint32_t xid) {
    RowLockTag tag;
    strcpy(tag.table_name, table);
    tag.oid = oid;

   // uint32_t h = hash_row_lock_tag(&tag);
    uint32_t h_raw = hash_row_lock_tag(&tag);
uint32_t h = h_raw % ROW_LOCK_BUCKETS;  
    LWLock* lock = &global_row_locks.bucket_locks[h];
if (h >= ROW_LOCK_BUCKETS) {
        fprintf(stderr, "❌ Hash %u out of bounds!\n", h);
       // exit(1);
        return false;
    }
    while(1){
    while (!__sync_bool_compare_and_swap(&(lock->state), 0, 1)) {
        // 0 表示空，1 表示有人持有
        // busy wait，自旋锁（可以加 pause 或 yield）
           sched_yield();  // 主动让出 CPU
    }

    RowLock* curr = global_row_locks.buckets[h];
    RowLock* prev = NULL;
    while (curr) {
        if (row_lock_tag_equal(&curr->tag, &tag)) {
            if (curr->holder_xid == 0 || curr->holder_xid == xid) {
                 printf("获得锁, xid=%d\n",xid);
                curr->holder_xid = xid;
                lock->state = 0;
                 return true;
            } else {
                // 已被别人占用，释放锁后重试
                printf("已被别人占用，释放锁后重试,xid=%d\n",xid);
                lock->state = 0;
                sched_yield();
                  sleep(1);
                goto retry;
               // return;
             
            }
        }
           // prev = curr;
        curr = curr->next;
    }

    // 创建新锁
    RowLock* new_lock = malloc(sizeof(RowLock));
      if (!new_lock) {
        lock->state = 0;
        return false;
    }
     printf("得到锁,xid=%d\n",xid);
    new_lock->tag = tag;
    new_lock->holder_xid = xid;
    new_lock->next = global_row_locks.buckets[h];
    global_row_locks.buckets[h] = new_lock;

    lock->state = 0;
      return true;
retry:
        continue;
}
}

void unlock_row(const char* table, uint32_t oid, uint32_t xid) {
    RowLockTag tag;
    strcpy(tag.table_name, table);
    tag.oid = oid;

       uint32_t h_raw = hash_row_lock_tag(&tag);
uint32_t h = h_raw % ROW_LOCK_BUCKETS; 
    LWLock* lock = &global_row_locks.bucket_locks[h];

    while (!__sync_bool_compare_and_swap(&lock->state, 0, 1)) {
        sched_yield();
    }

    RowLock* curr = global_row_locks.buckets[h];
    while (curr) {
        if (row_lock_tag_equal(&curr->tag, &tag)) {
            if (curr->holder_xid == xid) {
                curr->holder_xid = 0;
                break;
            }
        }
        curr = curr->next;
    }

    lock->state = 0;
}