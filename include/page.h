#ifndef PAGE_H
#define PAGE_H
#include <stdint.h>
#include "types.h"


void page_init(Page* page, PageID page_id);
//size_t page_free_space(const Page* page);
bool page_insert_tuple(Page* page, const Tuple* tuple, uint16_t* slot_out) ;

int read_page(const char *table_path, PageID page_id, Page *page);
int write_page(const char *table_path, const Page *page);
PageID allocate_page(const char *table_path);
int find_free_slot(const Page *page);
int add_tuple_to_page(Page *page, const Tuple *tuple);
int delete_tuple_from_page(Page *page, uint32_t tuple_oid);


#endif // PAGE_H