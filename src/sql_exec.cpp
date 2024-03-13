/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "sql_exec.h"
#include "EvalPlan.h"

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
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

QueryResult *SQLExec::insert(const InsertStatement *statement) {
    validate_table(statement->tableName, true);

    Identifier table_name = statement->tableName;
    // get the table from _tables
    DbRelation& table = tables->get_table(table_name);
    ColumnAttributes column_attributes;
    ColumnNames column_names;
    unsigned int index = 0;

    // if column names specified in the statement: INSERT INTO table_name (column1, column2, column3, ...) VALUES
    // add to column names, otherwise add all column names
    if(statement->columns != nullptr){
        for (auto const col : *statement->columns){
            column_names.push_back(col);
        }
    }
    else {
        for (auto const col: table.get_column_names()){
            column_names.push_back(col);
        }
    }

    if(column_names.size() != statement->values->size()) {
        throw SQLExecError(string("DbRelationError: Unmatching columns and values."));
    }

    IndexNames index_names = indices->get_index_names(table_name);
    ValueDict row; // row to be added
    try {
        for (unsigned int i = 0; i < column_names.size(); ++i) {
            Expr *value_expr = (*statement->values)[i];
            switch (value_expr->type) {
                case kExprLiteralInt:
                    row[column_names[i]] = Value(value_expr->ival);
                    break;
                case kExprLiteralString:
                    row[column_names[i]] = Value(value_expr->name);
                    break;
                default:
                    throw SQLExecError("Not supported data type");
            }
        }

        Handle handle = table.insert(&row);

        for (const auto &index_name : index_names) {
            DbIndex &index = indices->get_index(table_name, index_name);
            index.insert(handle);
        }
    } catch (exception &e) {
        throw SQLExecError(string("Insertion failed: ") + e.what());
    }

    string message = "successfully inserted 1 row into " + table_name;
    if (index_names.size() > 0)
        message += " and " + to_string(index_names.size()) + " indices";

    // del handle;
    return new QueryResult(message); 
}

ValueDict *SQLExec::get_where_conjunction(const Expr* node, ValueDict* conjunction = nullptr) {
    if (conjunction == nullptr)
    {
        conjunction = new ValueDict();
    }

    if (node->opType == Expr::OperatorType::AND) 
    {
        get_where_conjunction(node->expr, conjunction);
        get_where_conjunction(node->expr2, conjunction);

    } else if (node->opType == Expr::OperatorType::SIMPLE_OP && node->opChar == '=') 
    {
        if (node->expr2->type == kExprLiteralInt) 
        {
            (*conjunction)[node->expr->name] = Value(node->expr2->ival);
        } 
        else if (node->expr2->type == kExprLiteralString) 
        {
            (*conjunction)[node->expr->name] = Value(node->expr2->name);
        }
    }
    return conjunction;
}

QueryResult* SQLExec::del(const DeleteStatement* statement) {
    Identifier table_name = statement->tableName;
    DbRelation &table = tables->get_table(table_name); 
    
    EvalPlan* plan = new EvalPlan(table);
    if (statement->expr)
    {
        ValueDict* where = get_where_conjunction(statement->expr);
        plan = new EvalPlan(where, plan);
    }

    EvalPlan *optimized = plan->optimize();
    Handles* handles = optimized->pipeline().second;

    IndexNames indices = SQLExec::indices->get_index_names(statement->tableName);
    for (auto const &handle : *handles) 
    {
        for (auto const &index : indices)
        {
            SQLExec::indices->get_index(statement->tableName, index).del(handle);
        }
        table.del(handle);
    }

    string output =  "successfully deleted " + to_string(handles->size()) + " rows from " + table_name;
    if (indices.size() != 0) 
    {
        output += " and " + to_string(indices.size()) + " indices";
    } 
    delete plan;
    delete handles;
    delete optimized;
    return new QueryResult(output);
}

QueryResult *SQLExec::select(const SelectStatement *statement) {
    string table_name = statement->fromTable->name;
    DbRelation &table = tables->get_table(table_name); // prefer polymorphism

    ColumnNames *column_names = new ColumnNames(table.get_column_names());
    ColumnAttributes *column_attributes = new ColumnAttributes(table.get_column_attributes());

    EvalPlan *plan = new EvalPlan(table);
    if (statement->whereClause) {
        ValueDict *where = get_where_conjunction(statement->whereClause, table.get_column_names(), table.get_column_attributes());
        plan = new EvalPlan(where, plan);
    }

    if (statement->selectList && statement->selectList->size()) {

        ColumnNames *projection = get_select_projection(statement->selectList, table.get_column_names());
        plan = new EvalPlan(projection, plan);
        column_names = projection;
    }

    EvalPlan *optimized = plan->optimize();
    ValueDicts *rows = optimized->evaluate();

    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(rows->size()) + " rows\n");
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
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
 
    Columns *columns = dynamic_cast<Columns *>(&tables->get_table(Columns::TABLE_NAME));
    // check if columns exist
    for (auto &row : rows) {
        ValueDict where;
        where[COLUMN_NAME_COLUMN] = row[COLUMN_NAME_COLUMN];
        Handles * handles = columns->select(&where);
        int size = handles->size();
        delete handles;
        if (size == 0)
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

// FIXME: does not cover the case where you use a diff binary operation
void parse_where_clause(const hsql::Expr *node, ValueDict &c) {
    if (node->opType == Expr::OperatorType::AND) {
        Identifier left_col_ref = node->expr->expr->name;
        Identifier right_col_ref = node->expr2->expr->name;
        Expr *left_val = node->expr->expr2;
        Expr *right_val = node->expr2->expr2;

        c[std::string(left_col_ref)] = left_val->type == kExprLiteralInt ? Value(left_val->ival) : Value(left_val->name);
        c[std::string(right_col_ref)] = right_val->type == kExprLiteralInt ? Value(right_val->ival) :  Value(right_val->name);
    } else {
        c[std::string(node->expr->name)] = node->expr2->type == kExprLiteralInt ? Value(node->expr2->ival) : Value(node->expr2->name);
    }
}

ValueDict *SQLExec::get_where_conjunction(const hsql::Expr *where_clause, const ColumnNames &column_names, const ColumnAttributes &column_attribs) {
    // build a hashmap so we know which column_attribute belongs to a column_name.
    std::unordered_map<Identifier, ColumnAttribute> col_def; // column_definition
    for (int i = 0; i < column_names.size(); i++) {
        col_def[column_names[i]] = column_attribs[i];
    }

    ValueDict *conjunction = new ValueDict();
    parse_where_clause(where_clause, *conjunction);
    for (auto item: *conjunction) { 
        // verify that each (column_name, value) in the conjunction exists in target table's column definition
        auto col_it = col_def.find(item.first);
        if (col_it == col_def.end()) {
            throw SQLExecError("Could not find column " + item.first);
        }
        if (col_def[item.first].get_data_type() != item.second.data_type) {
            throw SQLExecError("Column: " + item.first + "'s data type does not match expected datatype");
        }
    }
    return conjunction;
}

ColumnNames *SQLExec::get_select_projection(const std::vector<hsql::Expr*>* list, const ColumnNames &column_names) {
    ColumnNames *projection = new ColumnNames();
    for (auto item : *list) {
        if (!item->name) continue;
        if (find(column_names.begin(), column_names.end(), string(item->name)) == column_names.end()) {
            throw SQLExecError("Could not find column " + string(item->name));
        }
        projection->push_back(string(item->name));
    }
    return projection;
}
