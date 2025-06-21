#include "minidb.h"
#include <assert.h>

void test_find_table_by_oid() {
    SystemCatalog catalog;
    init_system_catalog(&catalog);
    
    // 创建测试表
    ColumnDef cols[] = {
        {"id", INT4_TYPE},
        {"name", TEXT_TYPE}
    };
    
    int oid1 = create_table(&catalog, "table1", cols, 2);
    int oid2 = create_table(&catalog, "table2", cols, 2);
    
    // 查找表
    TableMeta *meta1 = find_table_by_oid(&catalog, oid1);
    assert(meta1 != NULL);
    assert(strcmp(meta1->name, "table1") == 0);
    
    TableMeta *meta2 = find_table_by_oid(&catalog, oid2);
    assert(meta2 != NULL);
    assert(strcmp(meta2->name, "table2") == 0);
    
    // 查找不存在的表
    assert(find_table_by_oid(&catalog, 9999) == NULL);
    
    printf("find_table_by_oid tests passed!\n");
}

int main() {
    test_find_table_by_oid();
    printf("All catalog tests passed!\n");
    return 0;
}