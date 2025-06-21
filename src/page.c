#include "page.h"
#include "tuple.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stddef.h> // 添加这行以支持ptrdiff_t

// 计算槽位数组起始位置
static Slot* page_slots(Page* page) {
    return (Slot*)(page->data);
}

// 计算槽位数组结束位置
static uint8_t* page_slots_end(Page* page) {
    return (uint8_t*)(page_slots(page) + page->header.slot_count);
}

// 计算数据区起始位置
static uint8_t* page_data_start(Page* page) {
    return page->data + page->header.free_start;
}

// 计算数据区结束位置
static uint8_t* page_data_end(Page* page) {
    return page->data + page->header.free_end;
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