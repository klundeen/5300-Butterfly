#include <cstring>
#include <numeric>
#include "heap_file.h"
#include "slotted_page.h"
#include "db_cxx.h"

#define DEBUG_ENABLED
#include "debug.h"

void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = this->get_new();
    this->put(page);
    delete page;
}

void HeapFile::drop(void) {
    this->close();
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

    db.put(nullptr, &key, block->get_block(), 0);  // write block #1 to the database
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
    this->dbfilename = std::string(home_dir) + this->name + ".db";
    this->db.set_re_len(DbBlock::BLOCK_SZ); // Set record length to 4K
    this->db.set_message_stream(_DB_ENV->get_message_stream());
    this->db.set_error_stream(_DB_ENV->get_error_stream());
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there
    this->closed = false;
}