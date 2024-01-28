#include <cstring>
#include <numeric>

#include "db_cxx.h"
#include "heap_table.h"

// #define DEBUG_ENABLED
#include "debug.h"

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) :
    DbRelation(table_name, column_names, column_attributes), file(table_name) {}

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
    this->open();
    ValueDict *val = this->validate(row);
    Handle handle = this->append(val);
    delete val;
    return handle;
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
        for (auto const& record_id: *record_ids) {
            handles->push_back(Handle(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    DEBUG_OUT("HeapTable::select(where) FIXME\n");
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids) {
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
    return this->project(handle, &this->column_names);
}


ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;

    SlottedPage *block = this->file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *rows = this->unmarshal(data);
    ValueDict *proj_rows = new ValueDict;

    for (auto const& col : *column_names)
    {
        auto it = rows->find(col);
        if (it != rows->end())
        {
            (*proj_rows)[col] = it->second;
        }
    }

    delete block;
    delete data;
    delete rows;
    return proj_rows;
}


ValueDict *HeapTable::validate(const ValueDict *row) {
    ValueDict *full_row = new ValueDict();
    for (auto const& col : this->column_names) {
        auto const &it = row->find(col);
        if (it == row->end()) {
            throw DbRelationError("Don't know how to handle this row...");
        }
        full_row->insert({col, it->second});
    }

    return full_row;
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

    delete data;
    delete block;

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
    Dbt *data = new Dbt(right_size_bytes, offset);

    delete[] bytes;
    delete[] right_size_bytes;
    return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data) {
    DEBUG_OUT("HeapTable::unmarshal()\n");
    ValueDict *row = new ValueDict();
    uint col_num = 0;
    u16 offset = 0;
    char *bytes = (char *)data->get_data();

    for (auto const &it : this->column_names) {
        ColumnAttribute cur_attribute = this->column_attributes[col_num++];
        DEBUG_OUT_VAR("Column: %s\n", it.c_str());
        DEBUG_OUT_VAR("Column Attr Type: %d\n", cur_attribute.get_data_type());
        switch (cur_attribute.get_data_type()) {
        case ColumnAttribute::DataType::INT:
        {
            DEBUG_OUT("Im a INT\n");
            int32_t val;
            memcpy(&val, bytes + offset, sizeof(val));
            offset += sizeof(val);
            (*row)[it] = Value(val);
            break;
        }
        case ColumnAttribute::DataType::TEXT:
        {
            DEBUG_OUT("I'm a TEXT\n");
            u16 size;
            memcpy(&size, bytes + offset, sizeof(size));
            offset += sizeof(size);
            std::string val(bytes + offset, size);
            (*row)[it] = Value(val);
            offset += size;
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
