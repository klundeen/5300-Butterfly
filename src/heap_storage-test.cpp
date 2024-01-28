
#include <cstring>
#include <numeric>

#include "db_cxx.h"
#include "storage_engine.h"
#include "slotted_page.h"
#include "heap_file.h"
#include "heap_table.h"
#include "heap_storage-test.h"

// #define DEBUG_ENABLED
#include "debug.h"

bool test_heap_storage() {
    DEBUG_OUT("Beginning of test_heap_storage\n");
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    DEBUG_OUT("Create and drop okay\n");
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    {
        DEBUG_OUT_VAR("value.n != 12, was: %d\n", value.n);
        return false;
    }
    value = (*result)["b"];
    if (value.s != "Hello!")
    {
        DEBUG_OUT("value.s != 'Hello!' was TRUE\n");
        return false;
    }
    table.drop();

    delete result;
    delete handles;

    return true;
}