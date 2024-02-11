/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "sql_exec.h"

using namespace std;
using namespace hsql;

// define constants
const string TABLE_NAME_COLUMN = "table_name";
const string COLUMN_NAME_COLUMN = "column_name";
const string DATA_TYPE_COLUMN = "data_type";
const string INDEX_NAME_COLUMN = "index_name";
const string SEQ_IN_INDEX_COLUMN = "seq_in_index";
const string INDEX_TYPE_COLUMN = "index_type";
const string IS_UNIQUE_COLUMN = "is_unique";

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;
// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name : *qres.column_names) out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++) out << "----------+";
        out << endl;
        for (auto const &row : *qres.rows) {
            for (auto const &column_name : *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (column_names) delete column_names;
    if (column_attributes) delete column_attributes;
    if (rows) {
        for (auto row : *rows) delete row;
        delete rows;
    }
}

QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if (!tables) tables = new Tables();
    if (!indices) indices = new Indices();
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *)statement);
            case kStmtDrop:
                return drop((const DropStatement *)statement);
            case kStmtShow:
                return show((const ShowStatement *)statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name,
                                ColumnAttribute &column_attribute) {
    throw SQLExecError("not implemented");  // FIXME
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            validate_table(statement->tableName, false);
            return create_table(statement);
        case CreateStatement::kIndex:
            validate_table(statement->tableName, true);
            return create_index(statement);
        default:
            throw SQLExecError("Not supported CREATE type");
    }
}

QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            validate_table(statement->name, true);
            return drop_table(statement);
        case DropStatement::kIndex:
            validate_table(statement->name, true);
            validate_index(statement->indexName, statement->name, true);
            return drop_index(statement);
        default:
            throw SQLExecError("Not supported DROP type");
    }
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            validate_table(statement->tableName, true);
            return show_columns(statement);
        case ShowStatement::kIndex:
            validate_table(statement->tableName, true);
            return show_index(statement);
        default:
            throw SQLExecError("Unknown type in SHOW statement");
    }
}

QueryResult *SQLExec::show_tables() {
    ColumnAttributes *column_attributes = new ColumnAttributes();
    ColumnNames *column_names = new ColumnNames();
    ValueDicts *rows = new ValueDicts();

    Handles *handles = tables->select();
    for (Handle const &handle : *handles) {
        ValueDict *row = tables->project(handle);
        if (row->at(TABLE_NAME_COLUMN).s != Columns::TABLE_NAME &&
            row->at(TABLE_NAME_COLUMN).s != Tables::TABLE_NAME &&
            row->at(TABLE_NAME_COLUMN).s != Indices::TABLE_NAME)
            rows->emplace_back(row);
        else
            delete row;
    }
    tables->get_columns(Tables::TABLE_NAME, *column_names, *column_attributes);

    delete handles;

    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(rows->size()) + " rows\n");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    ColumnAttributes *column_attributes = new ColumnAttributes();
    ColumnNames *column_names = new ColumnNames();
    ValueDicts *rows = new ValueDicts();

    Columns *columns = dynamic_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));
    for (auto const &column_name : columns->get_column_names())
        column_names->emplace_back(column_name);

    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(statement->tableName);
    Handles *handles = columns->select(&where);
    for (Handle const &handle : *handles) rows->emplace_back(columns->project(handle));

    delete handles;

    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(rows->size()) + " rows\n");
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnAttributes *column_attributes = new ColumnAttributes();
    ColumnNames *column_names = new ColumnNames();
    ValueDicts *rows = new ValueDicts();

    for (auto const &column_name : indices->get_column_names())
        column_names->emplace_back(column_name);

    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(statement->tableName);
    Handles *handles = indices->select(&where);
    for (Handle const &handle : *handles) rows->emplace_back(indices->project(handle));

    delete handles;

    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(rows->size()) + " rows\n");
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(statement->name);
    where[INDEX_NAME_COLUMN] = Value(statement->indexName);
    Handles *handles = indices->select(&where);
    for (Handle const &handle : *handles) indices->del(handle);

    delete handles;

    return new QueryResult("dropped index " + string(statement->indexName) + "\n");
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    string tableName = string(statement->name);
    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(tableName);

    HeapTable *table = dynamic_cast<HeapTable *>(&tables->get_table(tableName));
    // Remove all columns
    Columns *columns = dynamic_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));
    Handles *handles = columns->select(&where);
    for (Handle const &handle : *handles) columns->del(handle);
    delete handles;
    table->drop();
    // Remove table
    tables->del(*tables->select(&where)->begin());
    // Remove all indices
    handles = indices->select(&where);
    for (auto const &handle : *handles) indices->del(handle);
    delete handles;

    return new QueryResult("dropped " + tableName + "\n");
};

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    string tableName = string(statement->tableName);

    vector<ValueDict> rows;
    for (auto const &column : *statement->columns) {
        string type;
        if (column->type == ColumnDefinition::DataType::INT)
            type = "INT";
        else if (column->type == ColumnDefinition::DataType::TEXT) {
            type = "TEXT";
        } else {
            return new QueryResult("Not supported column type");
        }

        ValueDict col_row;
        col_row[TABLE_NAME_COLUMN] = Value(tableName);
        col_row[COLUMN_NAME_COLUMN] = Value(column->name);
        col_row[DATA_TYPE_COLUMN] = Value(type);
        rows.push_back(col_row);
    }

    ValueDict table_row;
    table_row[TABLE_NAME_COLUMN] = Value(tableName);

    Columns *columns = dynamic_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));

    tables->insert(&table_row);
    try {
        for (auto &row : rows) columns->insert(&row);
    } catch (DbRelationError &e) {
        Handles *handles = columns->select(&table_row);
        for (Handle const &handle : *handles) columns->del(handle);
        tables->del(*tables->select(&table_row)->begin());
        throw e;
    }

    HeapTable *table = dynamic_cast<HeapTable *>(&tables->get_table(tableName));
    table->create();

    return new QueryResult("created " + string(statement->tableName) + "\n");
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    if (statement->indexType && string(statement->indexType) != "BTREE" &&
        string(statement->indexType) != "HASH")
        throw SQLExecError("Unsupported index type " + string(statement->indexType));

    string indexType = statement->indexType ? string(statement->indexType) : "BTREE";

    int seq = 1;
    bool isUnique = true;
    vector<ValueDict> rows;
    for (char *const columnName : *statement->indexColumns) {
        ValueDict col_row;
        col_row[TABLE_NAME_COLUMN] = Value(statement->tableName);
        col_row[INDEX_NAME_COLUMN] = Value(statement->indexName);
        col_row[COLUMN_NAME_COLUMN] = Value(columnName);
        col_row[SEQ_IN_INDEX_COLUMN] = Value(seq++);
        col_row[INDEX_TYPE_COLUMN] = Value(indexType);
        col_row[IS_UNIQUE_COLUMN] = Value(isUnique);
        rows.push_back(col_row);
    }

    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(statement->tableName);
    // check if table exists
    Handles *handles = tables->select(&where);
    if (handles->size() == 0)
        throw SQLExecError("table " + string(statement->tableName) + " does not exist");
    delete handles;
    Columns *columns = dynamic_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));
    // check if columns exist
    for (auto &row : rows) {
        where[COLUMN_NAME_COLUMN] = row[COLUMN_NAME_COLUMN];
        if (columns->select(&where)->size() == 0)
            throw SQLExecError("Column " + row[COLUMN_NAME_COLUMN].s + " does not exist");
    }

    for (auto &row : rows) indices->insert(&row);

    return new QueryResult("created index " + string(statement->indexName) + "\n");
}

void SQLExec::validate_table(char* tableName, bool must_exists = true) {
    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(tableName);
    Handles *handles = tables->select(&where);
    int size = handles->size();
    delete handles;
    if (must_exists && !size)
        throw SQLExecError("table " + string(tableName) + " does not exist");
    if (!must_exists && size) throw SQLExecError("table " + string(tableName) + " exists ");
}

void SQLExec::validate_index(char* indexName, char* tableName, bool must_exists = true) {
    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(tableName);
    where[INDEX_NAME_COLUMN] = Value(indexName);
    Handles *handles = indices->select(&where);
    int size = handles->size();
    delete handles;
    if (must_exists && !size)
        throw SQLExecError("Index " + string(indexName) + " does not exist");
    if (!must_exists && size) 
        throw SQLExecError("Index " + string(indexName) + " exists ");
}