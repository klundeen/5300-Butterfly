#include <cstring>
#include <numeric>

#include "db_cxx.h"
#include "heap_storage.h"

#define DEBUG_ENABLED
#include "debug.h"

const char *DB_NAME = "butterfyl.db";

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

    return true;
}

typedef u_int16_t u16;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    DEBUG_OUT_VAR("SlottedPage(id: %d new:%d)\n", block_id, is_new);
    if (is_new) {
        DEBUG_OUT("New slotted page, initialize num_records and end_free\n");
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        DEBUG_OUT_VAR("Initial end_free: %u\n", this->end_free);
        put_header();
    } else {
        DEBUG_OUT("Existing slotted page\n");
        get_header(this->num_records, this->end_free);
        DEBUG_OUT_VAR("I think my free is: %u\n", this->end_free);
        DEBUG_OUT_VAR("And my record count: %u\n", this->num_records);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    DEBUG_OUT("SlottedPage::add()\n");
    if (!has_room(data->get_size()))
    {
        throw DbBlockNoRoomError("not enough room for new record");
    }
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    DEBUG_OUT_VAR("data size: %u\n", size);
    DEBUG_OUT_VAR("current end_free: %d\n", this->end_free);
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}


void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    DEBUG_OUT_VAR("get_header(id=%u)\n", id);
    size = this->get_n(4 * id);
    loc = this->get_n(4 * id + 2);
    DEBUG_OUT_VAR("Assigning size:%u loc:%u\n", size, loc);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    DEBUG_OUT_VAR("put_header(id:%u size:%u loc:%u)\n", id, size, loc);
    if (id == 0) { // called the put_header() version and using the default params
        DEBUG_OUT_VAR("Update num_records to:%u end_free:%u\n", this->num_records, this->end_free);
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

Dbt *SlottedPage::get(RecordID record_id) {
    u16 size = 0;
    u16 loc = 0;
    this->get_header(size, loc, record_id);

    if (loc == 0)
    {
        return NULL;
    }

    // char block[DbBlock::BLOCK_SZ];
    // std::memset(block, 0, sizeof(block));
    Dbt *data = new Dbt(address(loc), sizeof(block));
    return data;
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size = 0;
    u16 loc = 0;
    this->get_header(size, loc, record_id);

    u16 new_size = sizeof(data);
    if (new_size > size) {
    u16 extra = new_size - size;
        if (!this->has_room(extra)) {
            throw DbBlockNoRoomError("not enough room for new record");
        }
        this->slide(loc, loc - new_size);
        memcpy(this->address(loc - new_size), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
    }
    this->put_header();
    this->put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id) {
    u16 size = 0;
    u16 loc = 0;
    this->get_header(size, loc, record_id);

    this->put_header(record_id, 0, 0);
    this->slide(loc, loc + size);
}

RecordIDs *SlottedPage::ids(void) {
    RecordIDs* record_ids = new RecordIDs(this->num_records + 1);
    iota(record_ids->begin(), record_ids->end(), 1);

    return record_ids;
}

bool SlottedPage::has_room(u_int16_t size) {
    DEBUG_OUT_VAR("SlottedPage::has_room(%u)\n", size);
    DEBUG_OUT_VAR("End free:%u Num records:%u\n", this->end_free, this->num_records);
    u16 available = this->end_free - (this->num_records + 1) * 4;
    DEBUG_OUT_VAR("Avail: %u\n", available);
    return size <= available;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    u16 shift = end - start;
    if (0 == shift) {
        return;
    }
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

void HeapFile::create(void) {
    DEBUG_OUT("HeapFile::create()\n");
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = this->get_new();
    this->put(page);
}

void HeapFile::drop(void) {
    DEBUG_OUT("drop()\n");
    this->close();
    DEBUG_OUT_VAR("Attempting to remove: %s\n", this->dbfilename.c_str());
    remove(this->dbfilename.c_str());
}

void HeapFile::open(void) {
    DEBUG_OUT("HeapFile::open()\n");
    if (0 == this->last) {
        throw DbException("File does not exist\n");
    }
    this->db_open();
}

void HeapFile::close(void) {
    DEBUG_OUT("HeapFile::close()\n");
    this->db.close(0);
    this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    DEBUG_OUT("HeapFile::get_new()\n");
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage *HeapFile::get(BlockID block_id) {

    // FIXME THIS IS A PROBLEM WE AREN'T MAINTAINING PAGE STATE
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    SlottedPage *page = new SlottedPage(data, block_id);
    return page;
}

void HeapFile::put(DbBlock *block) {
    DEBUG_OUT("HeapFile::put()\n");

    int block_number;
    Dbt key(&block_number, sizeof(block_number));
    block_number = block->get_block_id();

    db.put(NULL, &key, block->get_block(), 0);  // write block #1 to the database
}

BlockIDs *HeapFile::block_ids() {
    BlockIDs* block_ids = new BlockIDs(this->last);
    iota(block_ids->begin(), block_ids->end(), 1);
    return block_ids;
}

// Note, requires _DB_ENV be initialized
// FIXME, should check if it is NULL befor using it...
void HeapFile::db_open(uint flags) {
    DEBUG_OUT_VAR("db_open(%d)\n", flags);
    if (!this->closed) {
        DEBUG_OUT("already open...\n");
        return;
    }

    const char *home_dir;
    _DB_ENV->get_home(&home_dir);
    DEBUG_OUT_VAR("home_dir?: %s\n", home_dir);
    this->dbfilename = std::string(home_dir) + "/" + this->name + ".db";
    DEBUG_OUT_VAR("dbfilename?: %s\n", this->dbfilename.c_str());
    this->db.set_re_len(DbBlock::BLOCK_SZ); // Set record length to 4K
    this->db.set_message_stream(_DB_ENV->get_message_stream());
    this->db.set_error_stream(_DB_ENV->get_error_stream());
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there
    this->closed = false;
}

//////////////////////////////////////////////////////////////////////////////////

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) :
    DbRelation(table_name, column_names, column_attributes), file(table_name)
{}

void HeapTable::create() {
    DEBUG_OUT("HeapTable::create()\n");
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    DEBUG_OUT("HeapTable::create_if_not_exists()\n");
    try {
        DEBUG_OUT("TRYING open()\n");
        this->file.open();
    } catch (DbException &exc) {
        DEBUG_OUT("CATCHING and creating\n");
        this->file.create();
    }
}

void HeapTable::drop() {
    this->file.drop();
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    DEBUG_OUT("HeapTable::insert()\n");
    this->open();
    return this->append(this->validate(row));
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {

}

void HeapTable::del(const Handle handle) {

}


Handles* HeapTable::select() {
    DEBUG_OUT("HeapTable::select() FIXME\n");
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    DEBUG_OUT("HeapTable::select(where) FIXME?\n");
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
        {
            // FIXME, need to only return WHERE qualified record...
            handles->push_back(Handle(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}


ValueDict *HeapTable::project(Handle handle) {
    DEBUG_OUT("HeapTable::project(handle) FIXME\n");
    return this->project(handle, NULL);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    DEBUG_OUT("HeapTable::project(handle, column_names ) FIXME\n");
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;

    SlottedPage *block = this->file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *rows = this->unmarshal(data);

    if (NULL == column_names) {
        return rows;
    }

    ValueDict *proj_rows = new ValueDict;
    for (auto const& col : *column_names)
    {
        auto it = rows->find(col);
        if (it != rows->end())
        {
            (*proj_rows)[col] = it->second;
        }
    }

    return rows;
}


ValueDict *HeapTable::validate(const ValueDict *row) {
    return (ValueDict*)row;
}

Handle HeapTable::append(const ValueDict *row) {
    DEBUG_OUT("HeapTable::append()\n");
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        DEBUG_OUT("Try to add...\n");
        record_id = block->add(data);
    } catch (DbException &exc) {
        DEBUG_OUT("Catch and get_new()...\n");
        block = this->file.get_new();
        record_id = block->add(data);
    }

    this->file.put(block);
    return Handle(this->file.get_last_block_id(), record_id);
}


// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict* row) {
    DEBUG_OUT("HeapTable::marshal()\n");
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data) {
    DEBUG_OUT("HeapTable::unmarshal() FIXME\n");
    ValueDict *row = new ValueDict();
    uint col_num = 0;
    u16 offset = 0;
    void *bytes = data->get_data();

    for (auto const &it : this->column_names) {
        ColumnAttribute cur_attribute = this->column_attributes[col_num++];
        DEBUG_OUT_VAR("Column: %s\n", (*it).c_str());
        DEBUG_OUT_VAR("Column Attr Type: %d\n", cur_attribute.get_data_type());
        switch (cur_attribute.get_data_type()) {
        case ColumnAttribute::DataType::INT:
        {
            DEBUG_OUT("Im a INT\n");
            // int32_t val;
            // memcpy(&val, bytes + offset, sizeof(val));
            // offset += sizeof(val);
            // (*row)[it] = Value(val);
            break;
        }
        case ColumnAttribute::DataType::TEXT:
        {
            DEBUG_OUT("I'm a TEXT\n");
            // u16 size;
            // memcpy(&size, bytes + offset, sizeof(size));
            // offset += sizeof(size);
            // std::string val(bytes + offset, size);
            // (*row)[it] = Value(val);
            // offset += size;
            break;
        }
        default:
        {
            DEBUG_OUT("Unsupported type...\n");
            break;
        }
        }
    }

    return row;
}
