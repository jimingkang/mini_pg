#ifndef LOCK_H
#define LOCK_H
#include "types.h"

bool LWLockAcquireExclusive(LWLock *lock) ;

void LWLockInit(LWLock *lock, uint16_t tranche_id);
#endif