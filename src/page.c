#include "page.h"
#include "tuple.h"
#include "lock.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stddef.h> // 添加这行以支持ptrdiff_t
extern const char *DATADIR;


// 计算槽位数组起始位置
//static Slot* page_slots(Page* page) {
 //   return (Slot*)(page->data);
//}

// 计算槽位数组结束位置
//static uint8_t* page_slots_end(Page* page) {
//    return (uint8_t*)(page_slots(page) + page->header.slot_count);
//}
static Slot* page_slots(Page* page) {
    return page->slots;
}

// 计算槽位数组结束位置（固定为 slots + MAX_SLOTS）
static uint8_t* page_slots_end(Page* page) {
    return (uint8_t*)(page->slots + MAX_SLOTS);
}


static uint8_t* page_data_start(Page* page) {
    return ((uint8_t*)page) + PAGE_DATA_OFFSET + page->header.free_start;
}
// 计算数据区起始位置
//static uint8_t* page_data_start(Page* page) {
//    return page->data + page->header.free_start;
//}

// 计算数据区结束位置
//static uint8_t* page_data_end(Page* page) {
 //   return page->data + page->header.free_end;
//}
static uint8_t* page_data_end(Page* page) {
    return ((uint8_t*)page) + PAGE_DATA_OFFSET + page->header.free_end;
}

// 初始化页面
void page_init(Page* page, PageID page_id) {
    memset(page, 0, sizeof(Page));
    
    page->header.checksum = 0;
    page->header.page_id = page_id;
    page->header.lsn = 0;
    page->header.free_start = 0;  // 数据区起始位置（初始为0）
    page->header.free_end = sizeof(page->data); // 数据区结束位置（初始为末尾）
    page->header.slot_count = 0;
    page->header.tuple_count = 0;
    page->header.next_page = INVALID_PAGE_ID;
    page->header.prev_page = INVALID_PAGE_ID;
}

// 计算页面空闲空间
size_t page_free_space(const Page* page) {
    // 空闲空间 = 数据区可用空间 + 未使用的槽位空间
    size_t data_space = page->header.free_end - page->header.free_start;
    size_t slot_space = (MAX_SLOTS - page->header.slot_count) * sizeof(Slot);
    return data_space + slot_space;
}

void free_page(Page* page) {
    if (page) free(page);
}
// 压缩页面（回收删除的空间）
void page_compact(Page* page) {
    uint8_t* new_data_start = page_slots_end(page);
    uint8_t* old_data_start = page_data_start(page);
    uint8_t* old_data_end = page_data_end(page);
    
    size_t data_size = old_data_end - old_data_start;
    
    if (new_data_start > old_data_start) {
        // 移动数据到新位置
        memmove(new_data_start, old_data_start, data_size);
        
        // 更新槽位的偏移量
        Slot* slots = page_slots(page);
        ptrdiff_t offset_diff = new_data_start - old_data_start;
        
        for (int i = 0; i < page->header.slot_count; i++) {
            if (slots[i].flags & SLOT_OCCUPIED) {
                slots[i].offset += offset_diff;
            }
        }
        
        // 更新空闲空间指针
        page->header.free_start = (new_data_start - page->data) + data_size;
        page->header.free_end = sizeof(page->data);
    }
}

// 插入元组到页面
bool page_insert_tuple(Page* page, const Tuple* tuple, uint16_t* slot_out) {
    if (!page || !tuple || !slot_out) return false;
    if (page->header.slot_count >= MAX_SLOTS) return false;

    
    // 序列化元组以确定所需空间
    uint8_t buffer[MAX_TUPLE_SIZE];
    size_t tuple_size = serialize_tuple(tuple, buffer);
    if (tuple_size == 0) return false;
    
    // 检查是否有足够空间
    size_t required_space = tuple_size + sizeof(Slot);
    if (page_free_space(page) < required_space) {
        // 尝试压缩页面以释放空间
        page_compact(page);
        if (page_free_space(page) < required_space) {
            return false; // 仍然没有足够空间
        }
    }
    
    // 分配新槽位
    uint16_t slot_index = page->header.slot_count;
    page->header.slot_count++;
    
    Slot* slots = page_slots(page);
    Slot* new_slot = &slots[slot_index];
    
    // 在数据区分配空间（从空闲空间开始处分配）
    uint16_t data_offset = page->header.free_start;
    page->header.free_start += tuple_size;
    
    // 设置槽位信息
    new_slot->offset = data_offset;
    new_slot->length = tuple_size;
    new_slot->flags = SLOT_OCCUPIED;
    
    // 复制元组数据到页面
    memcpy(page->data + data_offset, buffer, tuple_size);
    
    // 更新页面元数据
    page->header.tuple_count++;
    
    *slot_out = slot_index;
    return true;
}

// 从页面删除元组
bool page_delete_tuple(Page* page, uint16_t slot) {
    if (!page || slot >= page->header.slot_count) {
        return false;
    }
    
    Slot* slots = page_slots(page);
    Slot* target_slot = &slots[slot];
    
    if (!(target_slot->flags & SLOT_OCCUPIED)) {
        return false; // 槽位未被占用
    }
    
    // 标记为已删除（不立即回收空间）
    target_slot->flags |= SLOT_DELETED;
    target_slot->flags &= ~SLOT_OCCUPIED;
    
    // 更新页面元数据
    page->header.tuple_count--;
    
    return true;
}

// 从页面获取元组
Tuple* page_get_tuple(const Page* page, uint16_t slot, const TableMeta* meta) {
    if (!page || slot >= page->header.slot_count) {
        return NULL;
    }
    
    const Slot* slots = page_slots(page);
    const Slot* target_slot = &slots[slot];
    
    if (!(target_slot->flags & SLOT_OCCUPIED)) {
        return NULL; // 槽位未被占用
    }
    
    // 从页面数据区读取元组
    const uint8_t* tuple_data = page->data + target_slot->offset;
    
    // 反序列化元组
    Tuple* tuple = (Tuple*)malloc(sizeof(Tuple));
    if (!tuple) return NULL;
    
    if (deserialize_tuple(tuple, tuple_data) != target_slot->length) {
        free(tuple);
        return NULL; // 反序列化失败
    }
    
    return tuple;
}

// 更新页面中的元组
bool page_update_tuple(Page* page, uint16_t slot, const Tuple* new_tuple) {
    if (!page || !new_tuple || slot >= page->header.slot_count) {
        return false;
    }
    
    Slot* slots = page_slots(page);
    Slot* target_slot = &slots[slot];
    
    if (!(target_slot->flags & SLOT_OCCUPIED)) {
        return false; // 槽位未被占用
    }
    
    // 序列化新元组
    uint8_t buffer[MAX_TUPLE_SIZE];
    size_t new_size = serialize_tuple(new_tuple, buffer);
    if (new_size == 0) return false;
    
    // 如果新元组更小或大小相同，直接覆盖
    if (new_size <= target_slot->length) {
        memcpy(page->data + target_slot->offset, buffer, new_size);
        target_slot->length = new_size;
        return true;
    }
    
    // 新元组更大，需要重新分配空间
    // 先删除旧元组
    target_slot->flags |= SLOT_DELETED;
    target_slot->flags &= ~SLOT_OCCUPIED;
    page->header.tuple_count--;
    
    // 再插入新元组
    uint16_t new_slot;
    if (!page_insert_tuple(page, new_tuple, &new_slot)) {
        // 插入失败，恢复旧元组状态
        target_slot->flags &= ~SLOT_DELETED;
        target_slot->flags |= SLOT_OCCUPIED;
        page->header.tuple_count++;
        return false;
    }
    
    // 更新槽位索引（如果需要）
    // 注意：调用方应使用新的槽位索引
    return true;
}

// 查找包含指定 OID 的槽位
uint16_t page_find_slot_by_oid(const Page* page, uint32_t oid) {
    if (!page) return INVALID_SLOT;
    
    const Slot* slots = page_slots(page);
    
    for (uint16_t i = 0; i < page->header.slot_count; i++) {
        if ((slots[i].flags & SLOT_OCCUPIED)) {
            // 读取元组 OID（避免完整反序列化）
            const uint8_t* tuple_data = page->data + slots[i].offset;
            uint32_t tuple_oid;
            memcpy(&tuple_oid, tuple_data, sizeof(uint32_t));
            
            if (tuple_oid == oid) {
                return i;
            }
        }
    }
    
    return INVALID_SLOT;
}

// 打印页面信息
void page_print_info(const Page* page) {
    if (!page) {
        printf("NULL page\n");
        return;
    }
    
    printf("Page ID: %u\n", page->header.page_id);
    printf("Tuples: %u/%u (slots used: %u/%u)\n", 
           page->header.tuple_count, MAX_SLOTS,
           page->header.slot_count, MAX_SLOTS);
    printf("Free space: %zu bytes\n", page_free_space(page));
    printf("Data range: [%u, %u]\n", page->header.free_start, page->header.free_end);
    printf("LSN: %u\n", page->header.lsn);
    printf("Prev page: %u, Next page: %u\n", 
           page->header.prev_page, page->header.next_page);
    
    // 打印槽位信息
    printf("Slots:\n");
    const Slot* slots = page_slots(page);
    for (int i = 0; i < page->header.slot_count; i++) {
        printf("  Slot %d: %s, offset=%u, len=%u\n", 
               i,
               (slots[i].flags & SLOT_OCCUPIED) ? "occupied" : 
               (slots[i].flags & SLOT_DELETED) ? "deleted" : "free",
               slots[i].offset,
               slots[i].length);
    }
}

// ========== 新增：从文件中读取页面 ==========
// ========== 新增：从文件中读取页面（使用路径） ==========
Page* read_page(const char* table_path, PageID page_id) {
    if (!table_path) return NULL;
    
    FILE* file = fopen(table_path, "rb");
    if (!file) return NULL;

    size_t page_size = sizeof(Page);
    if (fseek(file, page_id * page_size, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }

    Page* page = malloc(page_size);
    if (!page) {
        fclose(file);
        return NULL;
    }

    if (fread(page, 1, page_size, file) != page_size) {
        free(page);
        fclose(file);
        return NULL;
    }

    fclose(file);
    return page;
}

void init_page_cache() {
    memset(&global_page_cache, 0, sizeof(global_page_cache));
    //pthread_mutex_init(&global_page_cache.lock, NULL);
     for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        global_page_cache.entries[i].dirty=false;
        global_page_cache.entries[i].valid=false;
        LWLockInit(&global_page_cache.entries[i].page.lock, TRANCHE_PAGE_LOCK);
    }
    LWLockInit(&global_page_cache.lock, TRANCHE_PAGE_LOCK);  // 全局锁也初始化




}

Page* page_cache_get(uint32_t oid, TableMeta* meta, FILE* table_file) {
    //pthread_mutex_lock(&global_page_cache.lock);
      LWLockAcquireExclusive(&global_page_cache.lock);
    // 查找缓存
    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        if (global_page_cache.entries[i].valid && global_page_cache.entries[i].oid == oid) {
            //pthread_mutex_unlock(&global_page_cache.lock);
            LWLockRelease(&global_page_cache.lock);
            return &global_page_cache.entries[i].page;
        }
    }

    // 没命中，从磁盘读取
    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        if (!global_page_cache.entries[i].valid) {
            long offset = (long)oid * sizeof(Page);
            fseek(table_file, offset, SEEK_SET);
            fread(&global_page_cache.entries[i].page, sizeof(Page), 1, table_file);
            global_page_cache.entries[i].oid = oid;
            global_page_cache.entries[i].valid = true;
            global_page_cache.entries[i].dirty = false;

            //pthread_mutex_unlock(&global_page_cache.lock);
             LWLockRelease(&global_page_cache.lock);
            return &global_page_cache.entries[i].page;
        }
    }

    //pthread_mutex_unlock(&global_page_cache.lock);
    LWLockRelease(&global_page_cache.lock);
    return NULL;  // 缓存满未命中
}
//page_id
void page_cache_mark_dirty(uint32_t page_id) {
    LWLockAcquireExclusive(&global_page_cache.lock);

    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        PageCacheEntry* entry = &global_page_cache.entries[i];
        if (entry->valid && entry->page.header.page_id == page_id) {
            entry->dirty = true;
            break;
        }
    }

    LWLockRelease(&global_page_cache.lock);
}
Page* page_cache_load_or_fetch(uint32_t page_id, const char* filename) {
    LWLockAcquireExclusive(&global_page_cache.lock);
    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        if (global_page_cache.entries[i].valid && global_page_cache.entries[i].page.header.page_id == page_id) {
            LWLockRelease(&global_page_cache.lock);
            return &global_page_cache.entries[i].page;
        }
    }
    FILE* fp = fopen(filename, "r+b");
    if (!fp) {
        perror("fopen failed");
        LWLockRelease(&global_page_cache.lock);
        return NULL;
    }
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    if ((long)(page_id * sizeof(Page)) >= file_size) {
        fclose(fp);
        LWLockRelease(&global_page_cache.lock);
        return NULL;
    }
    fseek(fp, page_id * sizeof(Page), SEEK_SET);
    Page page;
    if (fread(&page, sizeof(Page), 1, fp) != 1) {
        fclose(fp);
        LWLockRelease(&global_page_cache.lock);
        return NULL;
    }
    fclose(fp);
    int slot = -1;
    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        if (!global_page_cache.entries[i].valid) {
            slot = i;
            break;
        }
    }
    if (slot == -1) slot = rand() % PAGE_CACHE_SIZE;

    global_page_cache.entries[slot].page = page;
    global_page_cache.entries[slot].oid = page_id;
    global_page_cache.entries[slot].valid = true;
    global_page_cache.entries[slot].dirty = false;

    LWLockRelease(&global_page_cache.lock);
    return &global_page_cache.entries[slot].page;
}

bool page_cache_flush(uint32_t page_id, const char* filename) {
    LWLockAcquireExclusive(&global_page_cache.lock);
    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        if (global_page_cache.entries[i].valid && global_page_cache.entries[i].page.header.page_id == page_id) {
            if (!global_page_cache.entries[i].dirty) {
                LWLockRelease(&global_page_cache.lock);
                return true;
            }
            FILE* fp = fopen(filename, "r+b");
            if (!fp) {
                perror("flush fopen failed");
                LWLockRelease(&global_page_cache.lock);
                return false;
            }
            fseek(fp, page_id  * sizeof(Page), SEEK_SET);
            if (fwrite(&global_page_cache.entries[i].page, sizeof(Page), 1, fp) != 1) {
                fclose(fp);
                LWLockRelease(&global_page_cache.lock);
                return false;
            }
            fflush(fp);  // ✅ 可选：确保数据立即写入磁盘
            fclose(fp);
            global_page_cache.entries[i].dirty = false;
            LWLockRelease(&global_page_cache.lock);
            return true;
        }
    }
    LWLockRelease(&global_page_cache.lock);
    return false;
}


Page* old_page_cache_load_or_fetch(uint32_t oid, const char* filename) {
    //pthread_mutex_lock(&global_page_cache.lock);
    LWLockAcquireExclusive(&global_page_cache.lock);

    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        PageCacheEntry* entry = &global_page_cache.entries[i];
        if (entry->valid && entry->oid == oid) {
            //pthread_mutex_unlock(&global_page_cache.lock);
            LWLockRelease(&global_page_cache.lock);
            return &entry->page;
        }
    }

    // 没找到，装入缓存
    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        PageCacheEntry* entry = &global_page_cache.entries[i];
        if (!entry->valid) {
            //char file_path[MAX_NAME_LEN * 2];
          //  snprintf(file_path, sizeof(file_path), "%s/%s.tbl", DATADIR,filename);
            FILE* file = fopen(filename, "r+b");
            if (!file) {
                perror("Failed to open file for page cache fetch");
                //pthread_mutex_unlock(&global_page_cache.lock);
                 LWLockRelease(&global_page_cache.lock);
                return NULL;
            }

            if (fseek(file, oid * sizeof(Page), SEEK_SET) != 0) {
                perror("fseek error");
                fclose(file);
                //pthread_mutex_unlock(&global_page_cache.lock);
                 LWLockRelease(&global_page_cache.lock);
                return NULL;
            }

            if (fread(&entry->page, sizeof(Page), 1, file) != 1) {
                perror("fread error");
                fclose(file);
               // pthread_mutex_unlock(&global_page_cache.lock);
                LWLockRelease(&global_page_cache.lock);
                return NULL;
            }

            fclose(file);
            entry->oid = oid;
            entry->valid = true;
            entry->dirty = false;
            // 初始化页面锁
            entry->page.lock.state = 0;
            //pthread_mutex_unlock(&global_page_cache.lock);
            LWLockRelease(&global_page_cache.lock);
            return &entry->page;
        }
    }

    //pthread_mutex_unlock(&global_page_cache.lock);
    LWLockRelease(&global_page_cache.lock);
    fprintf(stderr, "Page cache full, cannot load page with oid=%u\n", oid);
    return NULL;
}

bool old_flush_page_cache(uint32_t oid, const char* filename) {
    LWLockAcquireExclusive(&global_page_cache.lock);
    for (int i = 0; i < PAGE_CACHE_SIZE; i++) {
        if (global_page_cache.entries[i].valid && global_page_cache.entries[i].oid == oid) {
            if (!global_page_cache.entries[i].dirty) {
                LWLockRelease(&global_page_cache.lock);
                return true;
            }
            //char file_path[MAX_NAME_LEN * 2];
            //snprintf(file_path, sizeof(file_path), "%s/%s.tbl", DATADIR,filename);
           // FILE* file = fopen(file_path, "rb");
            FILE* fp = fopen(filename, "r+b");
            if (!fp) {
                perror("flush fopen failed");
                LWLockRelease(&global_page_cache.lock);
                return false;
            }
            fseek(fp, oid * sizeof(Page), SEEK_SET);
            if (fwrite(&global_page_cache.entries[i].page, sizeof(Page), 1, fp) != 1) {
                fclose(fp);
                LWLockRelease(&global_page_cache.lock);
                return false;
            }
            fclose(fp);
            global_page_cache.entries[i].dirty = false;
            LWLockRelease(&global_page_cache.lock);
            return true;
        }
    }
    LWLockRelease(&global_page_cache.lock);
    return false;
}