/**
 * @file heap_storage.cpp
 * @author K Lundeen
 * @see Seattle University, CPSC5300
 */
#include <cstring>
#include "heap_storage.h"

// #define DEBUG_ENABLED
#include "debug.h"

#define PASS true
#define FAIL false

using namespace std;
typedef u_int16_t u16;

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
static bool test_slotted_page_burgi() {
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

/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assert_failure(string message) {
    cout << "FAILED TEST: " << message << endl;
    return false;
}

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise
 */
bool test_slotted_page1() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);

    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assert_failure("add id 1");

    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 1 back " + actual);

    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assert_failure("add id 2");

    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 2 back " + actual);

    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 2 back after expanding put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 1 back after expanding put of 1 " + actual);

    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 2 back after contracting put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 1 back after contracting put of 1 " + actual);

    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assert_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assert_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assert_failure("get of deleted record was not null");

    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assert_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assert_failure("wrong type thrown when add too big");
    }

    return true;
}

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise
 */
bool SlottedPage::test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);

    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assert_failure("add id 1");

    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 1 back " + actual);

    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assert_failure("add id 2");

    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 2 back " + actual);

    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 2 back after expanding put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 1 back after expanding put of 1 " + actual);

    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 2 back after contracting put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assert_failure("get 1 back after contracting put of 1 " + actual);

    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assert_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assert_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assert_failure("get of deleted record was not null");

    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assert_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assert_failure("wrong type thrown when add too big");
    }

    // more volume
    string gettysburg = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    int32_t n = -1;
    uint16_t text_length = gettysburg.size();
    uint total_size = sizeof(n) + sizeof(text_length) + text_length;
    char *data = new char[total_size];
    *(int32_t *) data = n;
    *(uint16_t *) (data + sizeof(n)) = text_length;
    memcpy(data + sizeof(n) + sizeof(text_length), gettysburg.c_str(), text_length);
    Dbt dbt(data, total_size);
    vector<SlottedPage> page_list;
    BlockID block_id = 1;
    Dbt slot_dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
    slot = SlottedPage(slot_dbt, block_id++, true);
    for (int i = 0; i < 10000; i++) {
        try {
            slot.add(&dbt);
        } catch (DbBlockNoRoomError &exc) {
            page_list.push_back(slot);
            slot_dbt = Dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
            slot = SlottedPage(slot_dbt, block_id++, true);
            slot.add(&dbt);
        }
    }
    page_list.push_back(slot);
    for (const auto &slot : page_list) {
        RecordIDs *ids = slot.ids();
        for (RecordID id : *ids) {
            Dbt *record = slot.get(id);
            if (record->get_size() != total_size)
                return assertion_failure("more volume wrong size", block_id - 1, id);
            void *stored = record->get_data();
            if (memcmp(stored, data, total_size) != 0)
                return assertion_failure("more volume wrong data", block_id - 1, id);
            delete record;
        }
        delete ids;
        delete[] (char *) slot.block.get_data();  // this is why we need to be a friend--just convenient
    }
    delete[] data;
    return true;
}

/**
 * Test helper. Sets the row's a and b values.
 * @param row to set
 * @param a column value
 * @param b column value
 */
void test_set_row(ValueDict &row, int a, string b) {
    row["a"] = Value(a);
    row["b"] = Value(b);
    row["c"] = Value(a % 2 == 0);  // true for even, false for odd
}

/**
 * Test helper. Compares row to expected values for columns a and b.
 * @param table    relation where row is
 * @param handle   row to check
 * @param a        expected column value
 * @param b        expected column value
 * @return         true if actual == expected for both columns, false otherwise
 */
bool test_compare(DbRelation &table, Handle handle, int a, string b) {
    ValueDict *result = table.project(handle);
    Value value = (*result)["a"];
    if (value.n != a) {
        delete result;
        return false;
    }
    value = (*result)["b"];
    if (value.s != b) {
		delete result;
        return false;
	}
    value = (*result)["c"];
    delete result;
    if (value.n != (a % 2 == 0))
        return false;
    return true;

}

/**
 * Testing function for heap storage engine.
 * @return true if the tests all succeeded
 */
bool test_heap_storage() {
     DEBUG_OUT("Beginning of test_heap_storage\n");

    if (!test_table_create_drop())
        return assertion_failure("table create/drop tests failed");
    cout << "table create/drop tests ok" << endl;
    
    if (!test_table_data())
        return assertion_failure("table data tests failed");
    cout << "table data tests ok" << endl;

    if (!test_file())
        return assertion_failure("file tests failed");
    cout << "file tests ok" << endl;
    
    if (!test_slotted_page_burgi())
        return assertion_failure("slotted page Burgi tests failed");
    cout << "slotted page Burgi tests ok" << endl;


    if (!test_slotted_page1())
        return assertion_failure("slotted page 1 tests failed");
    cout << endl << "slotted page 1 tests ok" << endl;

    char data[DbBlock::BLOCK_SZ];
    auto dbt = Dbt(&data, DbBlock::BLOCK_SZ);
    if (!SlottedPage(dbt, 1, true).test_slotted_page())
        return assertion_failure("slotted page 2 tests failed");
    cout << endl << "slotted page 2 tests ok" << endl;

    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    column_names.push_back("c");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::BOOLEAN);
    column_attributes.push_back(ca);

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exists ok" << endl;

    ValueDict row;
    string b = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    test_set_row(row, -1, b);
    table.insert(&row);
    cout << "insert ok" << endl;
    Handles *handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    cout << "select/project ok " << handles->size() << endl;
    delete handles;

    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "many inserts/select/projects ok" << endl;
    delete handles;

    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "del ok" << endl;
    table.drop();
    delete handles;

    return true;
}

