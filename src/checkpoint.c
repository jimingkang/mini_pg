#include "checkpoint.h"
#include "wal.h"
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>

#define CHECKPOINT_INTERVAL_SEC 10  // 每10秒执行一次

static pthread_t checkpoint_tid;

void *checkpoint_thread_fn(void *arg) {
    MiniDB *db = (MiniDB *)arg;

    while (1) {
        sleep(CHECKPOINT_INTERVAL_SEC);

        printf("[checkpoint thread] Running checkpoint...\n");
        wal_log_checkpoint(db);
    }

    return NULL;
}

void start_checkpoint_thread(MiniDB *db) {
    if (pthread_create(&checkpoint_tid, NULL, checkpoint_thread_fn, db) != 0) {
        perror("Failed to create checkpoint thread");
    } else {
        printf("[init] Checkpoint thread started\n");
    }
     pthread_join(checkpoint_tid, NULL);
}
