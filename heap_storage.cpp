#include "heap_storage.h"
bool test_heap_storage() {return true;}


//Slotted Page
typedef u_int16_t u16;
u_int16_t num_records;
u_int16_t end_free;

// Heap File
std::string dbfilename;
u_int32_t last;
bool closed;
Db db;

// Heap Table
HeapFile file;

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
 * Destructor
 */
SlottedPage::~SlottedPage() {}

/**
 *
 * @param other
 */
SlottedPage :: SlottedPage(const SlottedPage &other) {
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param temp
 */
SlottedPage :: SlottedPage(SlottedPage &&temp) {
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param other
 * @return
 */
SlottedPage &operator=(const SlottedPage &other){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param temp
 * @return
 */
SlottedPage &operator=(SlottedPage &temp) {
    /* FIXME FIXME FIXME */
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
 *
 * @param record_id
 * @return
 */
Dbt * SlottedPage::get(RecordID record_id){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param record_id
 * @param data
 */
void SlottedPage::put(RecordID record_id, const Dbt &data){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param record_id
 */
void SlottedPage::del(RecordID record_id){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @return
 */
RecordIDs * SlottedPage::ids(void){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param size
 * @param loc
 * @param id
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param id
 * @param size
 * @param loc
 */
// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

/**
 *
 * @param size
 * @return
 */
bool SlottedPage::has_room(u_int16_t size){
    /* FIXME FIXME FIXME */
}

void SlottedPage::slide(u_int16_t start, u_int16_t end){
    /* FIXME FIXME FIXME */
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

/**
 * Destructor
 */
HeapFile::~HeapFile() {}

/**
 *
 * @param other
 */
HeapFile::HeapFile(const HeapFile &other){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param temp
 */
HeapFile::HeapFile(HeapFile &&temp){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param other
 * @return
 */
HeapFile::HeapFile &operator=(const HeapFile &other){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param temp
 * @return
 */
HeapFile::HeapFile &operator=(HeapFile &&temp){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapFile::create(void){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapFile::drop(void){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapFile::open(void){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapFile::close(void){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @return
 */
SlottedPage * HeapFile::get_new(void){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param block_id
 * @return
 */
SlottedPage * HeapFile::get(BlockID block_id){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param block
 */
void HeapFile::put(DbBlock *block){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @return
 */
BlockIDs * HeapFile::block_ids(){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param flags
 */
void HeapFile::db_open(uint flags = 0){
    /* FIXME FIXME FIXME */
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

/**
 * Destructor
 */
HeapTable::~HeapTable() {}

/**
 *
 * @param other
 */
HeapTable::HeapTable(const HeapTable &other) {
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param temp
 */
HeapTable::HeapTable(HeapTable &&temp){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param other
 * @return
 */
HeapTable::HeapTable &operator=(const HeapTable &other){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param temp
 * @return
 */
HeapTable::HeapTable &operator=(HeapTable &&temp) {
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapTable::create(){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapTable::create_if_not_exists(){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapTable::drop(){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapTable::open(){
    /* FIXME FIXME FIXME */
}

/**
 *
 */
void HeapTable::close(){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param row
 * @return
 */
Handle HeapTable::insert(const ValueDict *row){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param handle
 * @param new_values
 */
void HeapTable::update(const Handle handle, const ValueDict *new_values){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param handle
 */
void HeapTable::del(const Handle handle){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @return
 */
Handles * HeapTable::select(){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param where
 * @return
 */
Handles * HeapTable::select(const ValueDict *where){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param handle
 * @return
 */
ValueDict * HeapTable::project(Handle handle){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param handle
 * @param column_names
 * @return
 */
ValueDict * HeapTable::project(Handle handle, const ColumnNames *column_names){
    /* FIXME FIXME FIXME */
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
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param row
 * @return
 */
Dbt * HeapTable::marshal(const ValueDict *row){
    /* FIXME FIXME FIXME */
}

/**
 *
 * @param data
 * @return
 */
ValueDict * HeapTable::unmarshal(Dbt *data){
    /* FIXME FIXME FIXME */
}