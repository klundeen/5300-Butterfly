/**
 * @file heap_storage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen, Dominic Burgi
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#pragma once

#include "db_cxx.h"
#include "storage_engine.h"
#include "slotted_page.h"
#include "heap_file.h"


/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

class HeapTable : public DbRelation {
public:
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

    virtual ~HeapTable() {}

    HeapTable(const HeapTable &other) = delete;

    HeapTable(HeapTable &&temp) = delete;

    HeapTable &operator=(const HeapTable &other) = delete;

    HeapTable &operator=(HeapTable &&temp) = delete;

    virtual void create();

    virtual void create_if_not_exists();

    virtual void drop();

    virtual void open();

    virtual void close();

    virtual Handle insert(const ValueDict *row);

    virtual void update(const Handle handle, const ValueDict *new_values);

    virtual void del(const Handle handle);

    virtual Handles *select();

    virtual Handles *select(const ValueDict *where);

    virtual ValueDict *project(Handle handle);

    virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

protected:
    HeapFile file;

    virtual ValueDict *validate(const ValueDict *row);

    virtual Handle append(const ValueDict *row);

    virtual Dbt *marshal(const ValueDict *row);

    virtual ValueDict *unmarshal(Dbt *data);
};

bool test_heap_storage();
