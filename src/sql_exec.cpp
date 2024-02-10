/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "sql_exec.h"

using namespace std;
using namespace hsql;

// define constraints
const string TABLE_NAME_COLUMN = "table_name";
const string COLUMN_NAME_COLUMN = "column_name";
const string DATA_TYPE_COLUMN = "data_type";

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
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

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    throw SQLExecError("not implemented");  // FIXME
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    if (statement->type != CreateStatement::kTable)
        return new QueryResult("Not supported create types");
    
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

    Columns *columns =
        static_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));
    
    tables->insert(&table_row);
    try {
        for (auto &row : rows) 
            columns->insert(&row);
    } catch (DbRelationError &e) {
        Handles *handles = columns->select(&table_row);
        for (Handle const &handle : *handles)
            columns->del(handle);
        tables->del(*tables->select(&table_row)->begin());
        throw e;
    }

    return new QueryResult("created " + string(statement->tableName) + "\n");
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    if (statement->type != DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");

    string tableName = string(statement->name);
    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(tableName);
    
    HeapTable *table = static_cast<HeapTable *>(&tables->get_table(tableName));
    
    Columns *columns =
        static_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));
    Handles * handles = columns->select(&where);
    for (Handle const &handle : *handles)
        columns->del(handle);
    
    delete handles;

    table->drop();
    
    tables->del(*tables->select(&where)->begin());

    return new QueryResult(string("dropped ") + tableName);  // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("Unknown type in SHOW statement");
    }
}

string return_msg(int num_rows) {
    return "successfully returned " + to_string(num_rows) + " rows\n";
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
    }
    tables->get_columns(Tables::TABLE_NAME, *column_names, *column_attributes);

    delete handles;

    return new QueryResult(column_names, column_attributes, rows,
                           return_msg(rows->size()));
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    ColumnAttributes *column_attributes = new ColumnAttributes();
    ColumnNames *column_names = new ColumnNames();
    ValueDicts *rows = new ValueDicts();

    Columns *columns =
        static_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));
    for (auto const &column_name : columns->get_column_names())
        column_names->emplace_back(column_name);

    ValueDict where;
    where[TABLE_NAME_COLUMN] = Value(statement->tableName);

    Handles *handles = columns->select(&where);
    for (Handle const &handle : *handles)
        rows->emplace_back(columns->project(handle));

    delete handles;

    return new QueryResult(column_names, column_attributes, rows,
                           return_msg(rows->size()));
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    return new QueryResult("show index not implemented");  // FIXME
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    return new QueryResult("drop index not implemented");  // FIXME
}
