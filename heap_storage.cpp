#include "heap_storage.h"
#include <stdio.h>
#include <iostream>
#include <cstring>
bool test_heap_storage() {
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
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();


}


//Slotted Page
typedef u_int16_t u16;
u_int16_t num_records;
u_int16_t end_free;
u_int32_t last;
bool closed;
/*
//Slotted Page


// Heap File
std::string dbfilename;

Db db;

// Heap Table
HeapFile file;
*/
/*
 * _______________________SLOTTED PAGE_________________________________________
 */

/**
 * Constructor
 * @param block
 * @param block_id
 * @param is_new
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
 *
 * @param data
 * @return
 */
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

/**
 * Get a record from the block. Return None if it has been deleted.
 * @param record_id
 * @return
 */
Dbt* SlottedPage::get(RecordID record_id){

    u16 size=(u16)this->num_records;
    u16 loc=(u16)this->end_free;
    this->get_header(size, loc,record_id);
    if(loc==0){
        return NULL;
    }
    //?? data->get_data() , how to unmarshall?
    return (Dbt*)memcpy(this->address(loc),NULL, size);
    //return memcpy(this->address(loc),Null, size); ?


}

/**
 * Replace the record with the given data. Raises ValueError if it won't fit.
 * @param record_id
 * @param data
 */
void SlottedPage::put(RecordID record_id, const Dbt &data){

    u16 size=(u16)this->num_records;
    u16 loc= (u16)this->end_free;
    this->get_header(size,loc,record_id);
    u16 new_size = (u16) data->get_size();
    if(new_size > size){
        u16 extra= new_size-size;
        if(!this->has_room(extra))
            throw DbBlockNoRoomError("not enough room for new record");
        this->slide(loc+new_size,loc+size);
        memcpy(this->address(loc-extra), data->get_data(),new_size);
    }
    else{
        memcpy(this->address(loc),data->get_data(),new_size);
        this->slide(loc+new_size,loc+size);
    }
    this->get_header(size,loc,record_id);
    this->put_header(record_id,size,loc);
}

/**
 *
 * @param record_id
 */
void SlottedPage::del(RecordID record_id){
    // Mark the given id as deleted by changing its size to zero and its location to 0.
    // Compact the rest of the data in the block. But keep the record ids the same for everyone.
    u16 size=(u16)this->num_records;
    u16 loc=(u16)this->end_free;
    this->get_header(size,loc,record_id);
    this->put_header(record_id,0,0);
    this->slide(loc,loc+size);

}

/**
 * Sequence of all non-deleted record ids.
 * @return
 */
RecordIDs * SlottedPage::ids(void){
    RecordIDs* ids;

    for( RecordID i=1;i<this->num_records+1;i++){
        u16 size=(u16)this->num_records;
        u16 loc=(u16)this->end_free;
        this->get_header(size, loc,i);
        if(loc!=0){
            ids->push_back(i);
        }

    }
    return ids;
}

/**
 * Find the size and offset for given id. For id of zero, it is the block header.
 * @param size
 * @param loc
 * @param id
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0){

    size=this->get_n(4*id);
    loc=this->get_n(4 * id + 2);
}

/**
 *  Store the size and offset for given id. For id of zero, store the block header.
 * @param id
 * @param size
 * @param loc
 */

void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

/**
 * Calculate if we have room to store a record with given size. The size should include the 4 bytes
   for the header, too, if this is an add.
 * @param size
 * @return
 */
bool SlottedPage::has_room(u_int16_t size){

    u_int16_t available=this->end_free-(this->num_records+1)*4;
    return size <= available;
}

/* If start < end, then remove data from offset start up to but not including offset end by sliding data
that is to the left of start to the right. If start > end, then make room for extra data from end to start
by sliding data that is to the left of start to the left.
Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
shift (end < start).
*/

void SlottedPage::slide(u_int16_t start, u_int16_t end){
    u_int16_t shift = end-start;
    if(shift==0){
        return;
    }
    //slide data ??
    memcpy(this->address(this->end_free+1+shift),memcpy(this->address(this->end_free+1),NULL,start),end);

    //fix headers

    RecordIDs::iterator<RecordID> id;

    for(id = this->ids()->begin(); id != this->ids()->end(); ++id){
        u_int16_t size=this->num_records;
        u_int16_t loc=this->end_free;
        this->get_header(size, loc,id);
        if(loc<=start){
            loc +=shift;
            this->put_header(id,size,loc);
        }
    }

    this->end_free +=shift;
    this->put_header();
}

/**
 *
 * @param offset
 * @return
 */
// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

/**
 *
 * @param offset
 * @param n
 */
// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

/**
 *
 * @param offset
 * @return
 */
// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

/*
 * _______________________HEAP FILE_________________________________________
 */


HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
    this->block_size= sizeof(name);//??

}


//Wrapper for Berkeley DB open, which does both open and creation.
void HeapFile::db_open(uint flags=0){

    if(this->closed== false){
        return;
    }
    this->db=db.DB();
    this->db.set_re_len(this->block_size);
    //QString path =
    this->dbfilename=(char*)_DB_ENV+(char*)this->name+'.db';
   // this->dbfilename=os.path.join(_DB_ENV,(char)this->name+'.db');
    auto dbtype= db.DB_RECNO;
    this->db.open(this->dbfilename,NULL,dbtype,flags);
    this->stat = this->db.stat(db.DB_FAST_STAT);
    this->last = this->stat['ndata']
    this->closed = false;
}
/**
 * Create physical file.
 */
void HeapFile::create(void){
    this->db_open(db.DB_CREATE | db.DB_EXCL);
    auto block = this->get_new(); //first block of the file
    this->put(block);
}

/**
 * Delete the physical file.
 */
void HeapFile::drop(void){

    this->close();
    remove((char*)this->dbfilename);
}

/**
 * Open physical file.
 */
void HeapFile::open(void){
    this->db_open();
    this->block_size= this->stat['re-len'];//??

}

/**
 * Close the physical file.
 */
void HeapFile::close(void){
    this->db.close();
    this->closed= true;
}

/**
 *Allocate a new block for the database file.
 *Returns the new empty DbBlock that is managing the records in this block and its block id.
 * @return
 */
SlottedPage* HeapFile::get_new(void){
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

/**
 * Get a block from the database file.
 * @param block_id
 * @return
 */
SlottedPage* HeapFile::get(BlockID block_id){
    return SlottedPage(this->db.get(block_id),block_id);
}

/**
 * Write a block back to the database file.
 * @param block
 */
void HeapFile::put(DbBlock *block){
    this->db.put(block->get_block_id(),to_bytes(block->get_block()))
}

/**
 * Sequence of all block ids.
 * @return
 */
BlockIDs* HeapFile::block_ids(){
    std::vector<BlockID>* ids;
    for(BlockID i =1;i<(BlockID)this->last+1;i++){
        ids->push_back(i);
    }
    return ids;
}



/*
 * _______________________HEAP Table_________________________________________
 */

/**
 *
 * @param table_name
 * @param column_names
 * @param column_attributes
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes){
    /* FIXME FIXME FIXME */
}


void HeapTable::create(){

    this->file.create();
} // TODO

/**
 *
 */
void HeapTable::create_if_not_exists(){

    try {
        this->open();
    } catch (db->DBNoSuchFileError) {
        this->create();
    }
} // TODO

/**
 *
 */
void HeapTable::drop(){
    this->file.drop();
} // TODO

/**
 *
 */
void HeapTable::open(){

    this->file.open();
} // TODO

/**
 *
 */
void HeapTable::close(){

    this->file.close();
} // TODO

/**
 *
 * @param row
 * @return
 */
Handle HeapTable::insert(const ValueDict *row){

    this->open();
    return this->append(this->validate(row));
} // TODO

/**
 *
 * @param handle
 * @param new_values
 */
void HeapTable::update(const Handle handle, const ValueDict *new_values){
    /* FIXME Not milestone2 */
}

/**
 *
 * @param handle
 */
void HeapTable::del(const Handle handle){
    /* FIXME Not milestone2 */
}

/**
 *
 * @return
 */
Handles * HeapTable::select(){
    /* FIXME Not milestone2 */
}

/**
 *
 * @param where
 * @return
 */
Handles * HeapTable::select(const ValueDict *where){
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

/**
 *
 * @param handle
 * @return
 */
ValueDict * HeapTable::project(Handle handle){
    /* FIXME Not milestone2 */
}

/**
 *
 * @param handle
 * @param column_names
 * @return
 */
ValueDict * HeapTable::project(Handle handle, const ColumnNames *column_names){
    /* FIXME Not milestone2 */
}

/**
 *
 * @param row
 * @return
 */
ValueDict * HeapTable::validate(const ValueDict *row){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param row
 * @return
 */
Handle HeapTable::append(const ValueDict *row){
    /* FIXME Not milestone2 */
}

/**
 *
 * @param row
 * @return
 */
Dbt * HeapTable::marshal(const ValueDict *row){
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

/**
 *
 * @param data
 * @return
 */
ValueDict * HeapTable::unmarshal(Dbt *data){
    /* FIXME Not milestone2 */
}