/**
 * CPSC-5300 Butterfly ,Sprint Verano:
 * @file heap_storage.cpp - Heap Storage Engine components
 * @author Shrividya Ballapadavu, Jara Lindsay
 * @author Kevin Lundeen, Supplied starting components
 */

#include "heap_storage.h"
#include <stdio.h>
#include <iostream>
#include <cstring>
#include <map>
#include "storage_engine.h"
#include <exception>
#include <utility>
#include <vector>
#include "db_cxx.h"

#define M_ToCharPtr(p)

typedef u_int16_t u16;
u_int16_t num_records;
u_int16_t end_free;
u_int32_t last;
bool closed;

// More type aliases
typedef std::string Identifier;
typedef std::vector <Identifier> ColumnNames;
typedef std::vector <ColumnAttribute> ColumnAttributes;
typedef std::pair <BlockID, RecordID> Handle;
typedef std::vector <Handle> Handles;
typedef std::map <Identifier, Value> ValueDict;

/*
 * ___________________________________SLOTTED PAGE_________________________________________
 * ________________________________________________________________________________________
 */

/**
 * Constructor for SlottedPage.
 * @param block Page from the database that is using SlottedPage.
 * @param block_id  u_int32_t BlockID.
 * @param is_new Bool to determine if new.
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}


/**
 * Add a new record to the block. Return its id.
 * @param data Dbt * Data given.
 * @return id added RecordID to return.
 */
RecordID SlottedPage::add(const Dbt *data) {
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

/**
 * Get a record from the block.
 * @param record_id RecordID to get.
 * @return Return Dbt * or None if it has been deleted.
 */
Dbt *SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0){
        return nullptr;
    }
    return new Dbt(this->address(loc), size);
}

/**
 * Replace the record with the given data. Raises DbBlockNoRoomError if it won't fit.
 * @param record_id RecordID to replace.
 * @param data Data to replace record with.
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) {

    u16 size = (u16) this->num_records;
    u16 loc = (u16) this->end_free;
    this->get_header(size, loc, record_id);
    u16 new_size = (u16) data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!this->has_room(extra))
            throw DbBlockNoRoomError("not enough room for new record");
        this->slide(loc + new_size, loc + size);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }
    this->get_header(size, loc, record_id);
    this->put_header(record_id, size, loc);
}

/**
 * Mark the given id as deleted by changing its size to zero and its location to 0.
 * Compact the rest of the data in the block. But keep the record ids the same for everyone.
 * @param record_id RecordID to mark deleted.
 */
void SlottedPage::del(RecordID record_id) {

    u16 size = (u16) this->num_records;
    u16 loc = (u16) this->end_free;
    this->get_header(size, loc, record_id);
    this->put_header(record_id, 0, 0);
    this->slide(loc, loc + size);

}

/**
 * Sequence of all non-deleted record ids.
 * @return ids RecordIDs * to all ids.
 */
RecordIDs *SlottedPage::ids(void) {
    RecordIDs *ids;

    for (RecordID i = 1; i < this->num_records + 1; i++) {
        u16 size = (u16) this->num_records;
        u16 loc = (u16) this->end_free;
        this->get_header(size, loc, i);
        if (loc != 0) {
            ids->push_back(i);
        }
    }
    return ids;
}

/**
 * Find the size and offset for given id. For id of zero, it is the block header.
 * @param size u_int16_t reference size of given id.
 * @param loc u_int16_t reference offset of given id.
 * @param id RecordID given id.
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {

    size = this->get_n(4 * id);
    loc = this->get_n(4 * id + 2);
}

/**
 * Store the size and offset for given id. For id of zero, store the block header.
 * @param id RecordID given id.
 * @param size u16 size of given id.
 * @param loc u16 offset for given id.
 */

void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

/**
 * Calculate if we have room to store a record with given size. The size should include the 4 bytes.
 * for the header, too, if this is an add.
 * @param size u_int16_t Size of the given record to store.
 * @return size True if room available, False otherwise.
 */
bool SlottedPage::has_room(u_int16_t size) {

    u_int16_t available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}

/*
*/
/**
 * If start < end, then remove data from offset start up to but not including offset end by sliding data
 * that is to the left of start to the right. If start > end, then make room for extra data from end to start
 * by sliding data that is to the left of start to the left.
 * Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
 * shift (end < start).
 * @param start u_int16_t The offset start.
 * @param end u_int16_t The offset end.
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    u_int16_t shift = end - start;
    if (shift == 0) {
        return;
    }
    //void *memcpy(void *dest, const void * src, size_t n)   void * source can't be null
    //Dbt data =new Dbt(this->address(this->end_free + 1), start);
    //memcpy(this->address(this->end_free + 1 + shift), data, end);
    memcpy(this->address(this->end_free + 1 + shift), memcpy(this->address(this->end_free + 1), NULL, start), end);

    //fix headers

    //RecordID::iterator id; //fix
    /*
    for(int i=0;i< this->ids()->size();i++){
        this->ids()[i]
    }
     */
    for (u16 i = 0; i < this->ids()->size(); i++) {
        u16 size = this->num_records;
        u16 loc = this->end_free;
        this->get_header(size, loc, this->ids()->at(i)); //fix
        if (loc <= start) {
            loc += shift;
            this->put_header(this->ids()->at(i), size, loc); //fix
        }
    }

    this->end_free += shift;
    this->put_header();
    //delete data;
}

/**
 * Get 2-byte integer at given offset in block.
 * @param offset u16 Given offset.
 * @return 2-byte integer at offset.
 */
u16 SlottedPage::get_n(u16 offset) {
    return *(u16 *) this->address(offset);
}

/**
 * Put a 2-byte integer at given offset in block.
 * @param offset u16 Given offset.
 * @param n 2-byte integer to put.
 */
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16 *) this->address(offset) = n;
}

/**
 * Make a void* pointer for a given offset into the data block.
 * @param offset u16 Given offset.
 * @return void * For given offset.
 */
void *SlottedPage::address(u16 offset) {
    return (void *) ((char *) this->block.get_data() + offset);
}

/*
 * ___________________________________Heap File_________________________________________
 * _____________________________________________________________________________________
 */

/**
 * Wrapper for Berkeley DB open, which does both open and creation.
 * @param flags uint flags for db.
 */
void HeapFile::db_open(uint flags) {

    if (this->closed == false) {
        return;
    }
    this->db.set_re_len(DbBlock::BLOCK_SZ); //fix
    //QString path =
    this->dbfilename = (char *) _DB_ENV + this->name + ".db"; //fix
    // this->dbfilename=os.path.join(_DB_ENV,(char)this->name+'.db');
    auto dbtype = DB_RECNO; //fix
    //Db::open(DbTxn *txnid, const char *file,
    //const char *database, DBTYPE type, u_int32_t flags, int mode);
    const char *fileName = this->dbfilename.c_str();
    this->db.open(NULL, fileName, NULL, dbtype, flags, 0);
    db.stat(NULL, NULL, DB_FAST_STAT); //fix
    this->last = this->db.stat(NULL, NULL, (u_int32_t) 'ndata'); //fix
    this->closed = false;
}

/**
 * Create physical file.
 */
void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    auto block = this->get_new(); //first block of the file
    this->put(block);
}

/**
 * Delete the physical file.
 */
void HeapFile::drop(void) {

    this->close();
    const char *fileName = this->dbfilename.c_str();
    remove(fileName);
    //remove(""+(char)this->dbfilename); //fix
}

/**
 * Open physical file.
 */
void HeapFile::open(void) {
    this->db_open();
    //this->block_size = this->db.stat(NULL, NULL, (u_int32_t) 're_len');//?? //fix

}

/**
 * Close the physical file.
 */
void HeapFile::close(void) {
    this->db.close(0); //Fix
    this->closed = true;
}

/**
 * Allocate a new block for the database file.
 * Returns the new empty DbBlock that is managing the records in this block and its block id.
 * @return page SlottedPage * SlottedPage containing the new empty DbBlock.
 */
SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

/**
 * Get a block from the database file.
 * @param block_id BlockIDof block to get.
 * @return SlottedPage * Page containing block from database file.
 */
SlottedPage *HeapFile::get(BlockID block_id) {
    //Db::get(DbTxn *txnid, Dbt *key, Dbt *data, u_int32_t flags);
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);

}

/**
 * Write a block back to the database file.
 * @param block DbBlock to write.
 */
void HeapFile::put(DbBlock *block) {
    // this->db.put(block->get_block_id(),to_bytes(block->get_block())) //Fix
    char *bytes = new char[DbBlock::BLOCK_SZ];
    bytes = (char *) block->get_block();
    BlockID block_id=block->get_block_id();
    Dbt key(&block_id, sizeof(block->get_block_id()));
    //Dbt data()
    //Db::put(DbTxn *txnid, Dbt *key, Dbt *data, u_int32_t flags);
    this->db.put(nullptr,&key, (Dbt *) bytes, 0); //Fix
    delete bytes;
}

/**
 * Sequence of all block ids.
 * @return ids BlockIds * Sequence.
 */
BlockIDs *HeapFile::block_ids() {
    std::vector <BlockID> *ids;
    for (BlockID i = 1; i < (BlockID) this->last + 1; i++) {
        ids->push_back(i);
    }
    return ids;
}

/*
* ___________________________________Heap Table_________________________________________
* ______________________________________________________________________________________
*/

/**
 * Constructor for HeapTable.
 * @param table_name Identifier for the table.
 * @param column_names ColumnNames vector of identifiers.
 * @param column_attributes ColumnAttributes vector of ColumnAttributes.
 */

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name) {
}


/**
 * Execute: CREATE TABLE <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create() {

    this->file.create();
}

/**
 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create_if_not_exists() {

    try {
        this->open();
    } catch (DbRelationError const &) {
        this->create();
    }
}

/**
 * Execute: DROP TABLE <table_name>
 */
void HeapTable::drop() {
    this->file.drop();
}

/**
 * Open existing table. Enables: insert, update, delete, select, project.
 */
void HeapTable::open() {
    this->file.open();
}

/**
 * Closes the table. Disables: insert, update, delete, select, project.
 */
void HeapTable::close() {

    this->file.close();
}

/**
 * Expect row to be a dictionary with column name key.
 * Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
 * @param row ValueDict * Map of Identifiers and Values.
 * @return Handle Return the handle of the inserted row.
 */
Handle HeapTable::insert(const ValueDict *row) {

    this->open();
    return this->append(this->validate(row));
}

/**
 * Expect new_values to be a dictionary with column name keys.
 * Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g., returned from an insert
 * or select).
 * @param handle BlockID RecordID pair to identify record.
 * @param new_values ValueDict * Map of Identifiers and Values.
 */
void HeapTable::update(const Handle handle, const ValueDict *new_values){
    /* FIXME Not milestone2: Added element to compile, please delete & implement */
    this->open();
}

/**
 * Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g., returned from an insert
 * or select).
 * @param handle BlockID RecordID pair to identify record.
 */
void HeapTable::del(const Handle handle){
    /* FIXME Not milestone2: Added element to compile, please delete & implement */
    this->open();
}

/**
 * Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
 * @return Handles * Returns a list of handles for qualifying rows.
 */

Handles * HeapTable::select(){
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id: *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**
 * Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
 * @param where ValueDict * Map of Identifiers and Values.
 * @return Handles * Returns a list of handles for qualifying rows.
 */
Handles *HeapTable::select(const ValueDict *where) {
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id: *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**
 * Return a sequence of values for handle given by column_names.
 * @param handle BlockID RecordID pair.
 * @return ValueDict * Map of Identifiers and Values.
 */
ValueDict * HeapTable::project(Handle handle){
    /* FIXME Not milestone2: Added element to compile, please delete & implement */
    ValueDict *valuedict = new ValueDict();
    return valuedict;
}

/**
 * Return a sequence of values for handle given by column_names.
 * @param handle BlockID RecordID pair.
 * @param column_names ColumnNames * vector of Identifiers
 * @return ValueDict * Map of Identifiers and Values.
 */

ValueDict * HeapTable::project(Handle handle, const ColumnNames *column_names){
    /* FIXME Not milestone2: Added element to compile, please delete & implement */
    ValueDict *valuedict = new ValueDict();
    return valuedict;
}


/**
 * Check if the given row is acceptable to insert. Raise ValueError if not.
 * Otherwise return the full row dictionary
 * @param row ValueDict * Map of Identifiers and Values.
 * @return full_row ValueDict * Map of Identifiers and Values.
 */
ValueDict *HeapTable::validate(const ValueDict *row) {
    std::map <Identifier, Value> *full_row = {};
    uint col_num = 0;
    for (auto const &column_name: column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name); //fix
        Value value = column->second;
        if (ca.get_data_type() != ColumnAttribute::DataType::INT &&
            ca.get_data_type() != ColumnAttribute::DataType::TEXT) {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        } else {
            value = row->at(column_name);

            //value = row[column_name]; //fix
            //full_row[column_name] = value; //fix
        }
    }
    return full_row;
}

/**
 * Assumes row is fully fleshed-out. Appends a record to the file.
 * @param row ValueDict * Map of Identifiers and Values.
 * @return Handle BlockID RecordID pair
 */
Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    u_int16_t record_id;
    try {
        record_id = block->add(data);
    } catch (DbRelationError const &) {
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    unsigned int id = this->file.get_last_block_id();
    std::pair <BlockID, RecordID> Handle(id, record_id);
    return Handle;
}

/**
 * Marshals given row into bytes and returns data as a Dbt *.
 * @param row ValueDict * Map of Identifiers and Values.
 * @return data Dbt * Marshaled data
 */
Dbt *HeapTable::marshal(const ValueDict *row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t * )(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16 *) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
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

/**
 * Unmarshals given data and returns row as a ValueDict *.
 * @param data Dbt * Marshaled data.
 * @return row ValueDict * Map of Identifiers and Values.
 */
ValueDict *HeapTable::unmarshal(Dbt *data) {
    std::map <Identifier, Value> *row = {};
    char *bytes = new char[DbBlock::BLOCK_SZ];
    char *buffer = new char[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint col_num = 0;
    Value value;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++]; //fix
        //ValueDict::const_iterator column = row->find(column_name); //fix
        //Value value = column->second; //fix
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.n = *(int32_t*)buffer;
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
                u16 size = *(u16 *) (bytes + offset);
                offset += sizeof(u16);
                char buffer[DbBlock::BLOCK_SZ];
                memcpy(buffer, bytes + offset, size);
                buffer[size] = '\0';
                value.s = std::string(buffer);
                offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    delete[] bytes;
    delete[] buffer;
    return row;
}

/*
 * ___________________________________Test_________________________________________
 * ________________________________________________________________________________
 */

/**
 * Tests heap_storage functionality to determine if implementations are correct.
 * @return True if tests pass, False otherwise.
 */
bool test_heap_storage() {

    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    std::cout << "Got to column names" << std::endl;

    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    std::cout << "Got to column attributes" << std::endl;

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    /*
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    /*
    ValueDict *result = table.project((*handles)[0]);

    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
        */
    //table.drop();
    //*/
    return true;
}