// heap_storage-test.cpp
// Simple test for heap storage implementation
// Dominic Burgi
// CPSC 5300 - Butterfly

#include "db_cxx.h"
#include "slotted_page.h"
#include "heap_file.h"
#include "heap_table.h"
#include "heap_storage-test.h"

// #define DEBUG_ENABLED
#include "debug.h"

#define PASS true
#define FAIL false

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

static bool create_drop(ColumnNames c_names, ColumnAttributes&c_attrs) {
    HeapTable table1("_test_create_drop_cpp", c_names, c_attrs);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;
    return PASS;
}

static bool test_data(ColumnNames c_names, ColumnAttributes&c_attrs) {
    HeapTable table("_test_data_cpp", c_names, c_attrs);
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
        return FAIL;
    }
    value = (*result)["b"];
    if (value.s != "Hello!")
    {
        DEBUG_OUT("value.s != 'Hello!' was TRUE\n");
        return FAIL;
    }
    table.drop();

    delete result;
    delete handles;
    return PASS;
}

bool test_heap_storage() {
    DEBUG_OUT("Beginning of test_heap_storage\n");

    ColumnNames column_names;
    ColumnAttributes column_attributes;
    initialize_columns(column_names, column_attributes);

    if (create_drop(column_names, column_attributes) == FAIL) {return FAIL;}
    if (test_data(column_names, column_attributes) == FAIL) {return FAIL;}

    return PASS;
}