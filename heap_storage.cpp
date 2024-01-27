#include <cstring>
#include <numeric>

#include "db_cxx.h"
#include "heap_storage.h"

bool test_heap_storage() {return true;}
/* FIXME FIXME FIXME */

typedef u_int16_t u16;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}


void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    size = this->get_n(4 * id);
    loc = this->get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
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

    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt *data = new Dbt(block, sizeof(block));
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
        this->slide(loc + new_size, loc + size);
        /*
        Store the actual data in the byte array...
        memcpy(this->address(loc), data->get_data(), size);
        */
    } else {
        /*
        Store the actual data in the byte array...
        memcpy(this->address(loc), data->get_data(), size);
        */
        this->slide(loc + new_size, loc + size);
    }
    this->get_header(size, loc, record_id);
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
    RecordIDs *data = new RecordIDs();
    return data;
}

bool SlottedPage::has_room(u_int16_t size) {
    u16 available = this->end_free - (this->num_records + 1) * 4;
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
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = this->get_new();
    this->put(page);
}

void HeapFile::drop(void) {

}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
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
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));
    Dbt key(&block_id, sizeof(block_id));

    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

void HeapFile::put(DbBlock *block) {
    // FIXME
}

BlockIDs *HeapFile::block_ids() {
    BlockIDs* block_ids = new BlockIDs(this->last);
    iota(block_ids->begin(), block_ids->end(), 1);
    return block_ids;
}

void HeapFile::db_open(uint flags) {
    if (!this->closed) {
        return;
    }

    // FIXME
    // this->db = bdb.DB();
}

//////////////////////////////////////////////////////////////////////////////////


void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->file.open();
    } catch (DbException &exc) {
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
    this->open();
    return this->append(this->validate(row));
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {

}

void HeapTable::del(const Handle handle) {

}


Handles* HeapTable::select() {
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


ValueDict *HeapTable::project(Handle handle) {
    return this->project(handle, NULL);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;

    SlottedPage *block = this->file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = this->unmarshal(data);

    if (NULL == column_names) {
        return row;
    }

    // FIXME
    // If we have column_names, we need to use them to project...
    return row;
}


ValueDict *HeapTable::validate(const ValueDict *row) {
    return (ValueDict*)row;
}

Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbException &exc) {
        SlottedPage *block = this->file.get_new();
        record_id = block->add(data);
    }

    this->file.put(block);
    Handle handle(this->file.get_last_block_id(), record_id);
    return handle;
}


// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict* row) {
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

    ValueDict *row = new ValueDict();
    // u16 offset = 0;
    // for (auto& it : this->column_names) {

    // }

    return row;

}
