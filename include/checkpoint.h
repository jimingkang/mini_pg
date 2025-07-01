#ifndef CHECKPOINT_H
#define CHECKPOINT_H

#include "minidb.h"

void *checkpoint_thread_fn(void *arg);  // 启动线程用
void start_checkpoint_thread(MiniDB *db); // 外部调用启动

#endif
