#ifndef PAGE_H
#define PAGE_H
#include <stdint.h>
#include <stddef.h>
#include "types.h"

#define PAGE_DATA_OFFSET offsetof(Page, data)
#define TRANCHE_PAGE_LOCK 1
#define TRANCHE_ROW_LOCK  2

#define PAGE_CACHE_SIZE 128  // 缓存页数上限，可按需调整

typedef struct PageCacheEntry {
    uint32_t oid;            // 页面所在行的 OID（或对应的唯一页标识符）=page_id
    Page page;              // 缓存的页面内容
    bool dirty;             // 是否被修改过，需写回磁盘
    bool valid;             // 是否为有效缓存
} PageCacheEntry;

typedef struct PageCache {
    PageCacheEntry entries[PAGE_CACHE_SIZE];
    LWLock lock;    // 多线程访问保护
} PageCache;

PageCache global_page_cache;



void page_init(Page* page, PageID page_id);
size_t page_free_space(const Page* page);

bool page_insert_tuple(Page* page, const  Tuple* tuple, uint16_t* slot_out);
bool page_delete_tuple(Page* page, uint16_t slot);
Tuple* page_get_tuple(const Page* page, uint16_t slot, const  TableMeta* meta);
bool page_update_tuple(Page* page, uint16_t slot, const  Tuple* new_tuple);
uint16_t page_find_slot_by_oid(const Page* page, uint32_t oid);
void page_print_info(const Page* page);

//int read_page(const char *table_path, PageID page_id, Page *page);
Page* read_page(const char* table_path, PageID page_id);
void free_page(Page* page) ;


//cache
void init_page_cache();
Page* page_cache_load_or_fetch(uint32_t oid, const char* filename) ;
Page* page_cache_get(uint32_t oid, TableMeta* meta, FILE* table_file) ;
bool page_cache_flush(uint32_t oid, const char* filename);
void page_cache_mark_dirty(uint32_t oid);
#endif // PAGE_H