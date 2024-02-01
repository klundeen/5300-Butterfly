// heap_storage-test.cpp
// Simple test for heap storage implementation
// Dominic Burgi
// CPSC 5300 - Butterfly

#include <cstring>
#include "db_cxx.h"
#include "slotted_page.h"
#include "heap_file.h"
#include "heap_table.h"
#include "heap_storage-test.h"

// #define DEBUG_ENABLED
#include "debug.h"

#define PASS true
#define FAIL false

using namespace std;

// Initialize columns for a table
static void initialize_columns(ColumnNames &c_names, ColumnAttributes &c_attrs) {
    // Names
    c_names.push_back("a");
    c_names.push_back("b");

    // Attributes
    ColumnAttribute ca(ColumnAttribute::INT);
    c_attrs.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    c_attrs.push_back(ca);
}

// Tests that HeapTable tables can be craeted and dropped
static bool test_table_create_drop() {
    DEBUG_OUT("===== Testing HeapTable : Create/Drop =====\n");
    ColumnNames c_names;
    ColumnAttributes c_attrs;
    initialize_columns(c_names, c_attrs);

    HeapTable table1("_test_table_create_drop_cpp", c_names, c_attrs);
    table1.create();
    DEBUG_OUT("create ok\n");
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    DEBUG_OUT("drop ok\n");
    return PASS;
}

// Tests data insertion and retrieval from HeapTable
static bool test_table_data() {
    DEBUG_OUT("===== Testing HeapTable : Data Usage =====\n");
    ColumnNames c_names;
    ColumnAttributes c_attrs;
    initialize_columns(c_names, c_attrs);
    HeapTable table("_test_data_cpp", c_names, c_attrs);
    table.create_if_not_exists();
    DEBUG_OUT("create_if_not_exsts ok\n");

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    DEBUG_OUT("try insert\n");
    table.insert(&row);
    DEBUG_OUT("insert ok\n");
    Handles* handles = table.select();
    DEBUG_OUT_VAR("select ok (size:%lu)\n", handles->size());
    ValueDict *result = table.project((*handles)[0]);
    DEBUG_OUT("project ok\n");
    if (nullptr == result)
    {
        DEBUG_OUT("project returned null\n.");
        return FAIL;
    }
    Value value = (*result)["a"];
    if (12 != value.n)
    {
        DEBUG_OUT_VAR("value.n != 12, was: %d\n", value.n);
        return FAIL;
    }
    value = (*result)["b"];
    if ("Hello!" != value.s)
    {
        DEBUG_OUT("value.s != 'Hello!' was TRUE\n");
        return FAIL;
    }
    table.drop();

    delete result;
    delete handles;
    return PASS;
}

// Tests HeapFile functionality
static bool test_file() {
    DEBUG_OUT("===== Testing HeapFile =====\n");

    uint expectedPageId = 1;

    HeapFile hf("_file_test");
    hf.create();
    hf.open();
    if (expectedPageId != hf.get_last_block_id()) {
        DEBUG_OUT("last block id was not 1...\n");
        return FAIL;
    }
    DEBUG_OUT("open and create ok\n");

    SlottedPage* page = hf.get_new();
    expectedPageId++;

    if (nullptr == page) {
        DEBUG_OUT("get_new() returned a NULL page...\n");
        return FAIL;
    }
    if (expectedPageId != hf.get_last_block_id()) {
        DEBUG_OUT("get_new() did not increment block id...\n");
        return FAIL;
    }
    DEBUG_OUT("get_new ok\n");

    uint curBlockId = page->get_block_id();
    SlottedPage* retrieved = hf.get(curBlockId);
    if(nullptr == retrieved) {
        DEBUG_OUT("retrieve attempt returned NULL page...\n");
        return FAIL;
    }

    if(curBlockId != retrieved->get_block_id()) {
        DEBUG_OUT("retrieved block ID did not match requested...\n");
        return FAIL;
    }
    DEBUG_OUT("get ok\n");

    hf.put(retrieved);
    if(curBlockId != retrieved->get_block_id()) {
        DEBUG_OUT("retrieved block ID did not match expected...\n");
        return FAIL;
    }
    DEBUG_OUT("put ok\n");

    delete retrieved;
    delete page;

    hf.drop();
    DEBUG_OUT("drop ok\n");
    return PASS;
}

// Test SlottedPage functionality
static bool test_slotted_page() {
    DEBUG_OUT("===== Testing SlottedPage =====\n");
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt dbt(block, sizeof(block));
    SlottedPage page(dbt, 1, true);

    const char *data = "foo";
    Dbt data_dbt((void *)data, strlen(data) + 1);
    RecordID rec_id = page.add(&data_dbt);
    if (1 != rec_id) {
        DEBUG_OUT("add's returned record ID did not match expected...\n");
        return FAIL;
    }
    DEBUG_OUT("add ok\n");

    Dbt *retrieved = page.get(rec_id);
    if (strcmp((char *)retrieved->get_data(), data) != 0)
    {
        DEBUG_OUT_VAR("failed to retrieve record (id: %u)\n", rec_id);
        return FAIL;
    }
    DEBUG_OUT("get_data ok\n");
    delete retrieved;

    const char *big_data = "foo bar baz";
    Dbt put_dbt((void *)big_data, std::strlen(big_data) + 1);
    page.put(rec_id, put_dbt);
    DEBUG_OUT("put ok\n");

    retrieved = page.get(rec_id);
    if (strcmp((char *)retrieved->get_data(), big_data) != 0)
    {
        DEBUG_OUT_VAR("failed to retrieve record (id: %u)\n", rec_id);
        return FAIL;
    }
    delete retrieved;
    page.del(rec_id);

    return PASS;
}

// Overall test wrapper for heap-storage classes
bool test_heap_storage() {
    DEBUG_OUT("Beginning of test_heap_storage\n");

    bool result = (test_table_create_drop() &&
                   test_table_data()        &&
                   test_file()              &&
                   test_slotted_page());

    return result;
}