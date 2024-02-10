/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "sql_exec.h"

using namespace std;
using namespace hsql;

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
    delete column_names;
    delete column_attributes;
    for (auto row : *rows) delete row;
    delete rows;
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
    return new QueryResult("not implemented"); // FIXME
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    return new QueryResult("not implemented");  // FIXME
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
    for (Handle const &handle : *handles)
        rows->emplace_back(tables->project(handle));
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
    where["table_name"] = string(statement->tableName);

    Handles *handles = columns->select(&where);
    for (Handle const &handle : *handles)
        rows->emplace_back(columns->project(handle));

    delete handles;

    return new QueryResult(column_names, column_attributes, rows,
                           return_msg(rows->size()));
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
     return new QueryResult("show index not implemented"); // FIXME
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    return new QueryResult("drop index not implemented");  // FIXME
}
