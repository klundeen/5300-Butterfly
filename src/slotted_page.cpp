/**
 * @file slotted_page.cpp - Implementation of storage_engine block.
 * SlottedPage: DbBlock
 *
 * @author Kevin Lundeen, Dominic Burgi
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include <cstring>
#include <numeric>
#include "heap_table.h"
#include "slotted_page.h"
#include "db_cxx.h"

// #define DEBUG_ENABLED
#include "debug.h"

const uint8_t HEADER_SIZE = 4;

// Manage a database block that contains several records.
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
    if (!has_room(data->get_size() + HEADER_SIZE))
    {
        throw DbBlockNoRoomError("not enough room for new record + header");
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

// Get the size and offset for given id. For id of zero, it is the block header.
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    DEBUG_OUT_VAR("get_header(id=%u)\n", id);
    size = this->get_n(HEADER_SIZE * id);
    loc = this->get_n(HEADER_SIZE * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    DEBUG_OUT_VAR("put_header(id:%u size:%u loc:%u)\n", id, size, loc);
    if (id == 0) { // called the put_header() version and using the default params
        DEBUG_OUT_VAR("Update num_records to:%u end_free:%u\n", this->num_records, this->end_free);
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(HEADER_SIZE*id, size);
    put_n(HEADER_SIZE*id + 2, loc);
}

// Get a record from the block. Return nullptr if it has been deleted.
Dbt *SlottedPage::get(RecordID record_id) {
    DEBUG_OUT_VAR("SlottedPage::get(%u)\n", record_id);
    u16 size = 0;
    u16 loc = 0;
    this->get_header(size, loc, record_id);

    if (loc == 0)
    {
        return nullptr;
    }

    Dbt *data = new Dbt(address(loc), sizeof(block));
    return data;
}

// Replace the record with the given data. Raises DbBlockNoRoomError if it won't fit.
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    DEBUG_OUT_VAR("SlottedPage::put(%u)\n", record_id);
    u16 size = 0;
    u16 loc = 0;
    this->get_header(size, loc, record_id);

    u16 new_size = sizeof(data);
    if (new_size > size) {
    u16 extra = new_size - size;
        if (!this->has_room(extra)) {
            throw DbBlockNoRoomError("Not enough room for new record");
        }
        this->slide(loc, loc - new_size);
        memcpy(this->address(loc - new_size), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
    }
    this->put_header();
    this->put_header(record_id, new_size, loc);
}

// Mark the given id as deleted by changing its size to zero and its location to 0.
// Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id) {
    DEBUG_OUT_VAR("SlottedPage::del(%u)\n", record_id);
    u16 size = 0;
    u16 loc = 0;
    this->get_header(size, loc, record_id);
    this->put_header(record_id, 0, 0);
    this->slide(loc, loc + size);
}

// Sequence of all non-deleted record ids.
RecordIDs *SlottedPage::ids(void) {
    DEBUG_OUT("SlottedPage::ids()\n");
    RecordIDs *record_ids = new RecordIDs();
    for (RecordID id = 1; id <= this->num_records; id++)
    {
        u_int16_t size = 0;
        u_int16_t loc = 0;
        this->get_header(size, loc, id);
        if (loc != 0)
        {
            record_ids->push_back(id);
        }
    }

    return record_ids;
}

// Calculate if we have room to store a record with given size.
// The size should include the 4 bytes for the header, too, if this is an add.
bool SlottedPage::has_room(u_int16_t size) {
    DEBUG_OUT_VAR("SlottedPage::has_room(%u)\n", size);
    DEBUG_OUT_VAR("End free:%u Num records:%u\n", this->end_free, this->num_records);
    u16 available = this->end_free - (this->num_records + 1) * HEADER_SIZE;
    DEBUG_OUT_VAR("Avail: %u\n", available);
    return size <= available;
}

// If start < end, then remove data from offset start up to but not including offset end by sliding data
// that is to the left of start to the right. If start > end, then make room for extra data from end to start
// by sliding data that is to the left of start to the left.
// Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
// shift (end < start).
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    DEBUG_OUT("SlottedPage::slide()\n");
    int shift = end - start;
    if (0 == shift) {
        return;
    }

    // Identify the start of the block, and move it
    u16 block_start = this->end_free + 1;
    memmove(address(block_start + shift), address(block_start), abs(shift));

    // Shift each record within the block
    RecordIDs *records = this->ids();
    for (RecordID &it : *records) {
        u16 size = 0;
        u16 loc = 0;
        get_header(size, loc, it);
        if (loc <= start) {
            loc += shift;
            put_header(it, size, loc);
        }
    }

    // Record updates back to the block header
    this->end_free += shift;
    put_header();
    delete records;
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

// Returns the number of records in the slotted page.
u_int16_t SlottedPage::get_num_records() {
    return this->num_records;
}

// Returns the end of the free space in the slotted page.
u_int16_t SlottedPage::get_end_free() {
    return this->end_free;
}