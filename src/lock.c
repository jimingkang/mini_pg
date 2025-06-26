
#include "minidb.h"

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

