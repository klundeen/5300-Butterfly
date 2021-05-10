/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */

/**
 *Milestone 3 & 4
 *File modified By: Nicholas Nguyen, Yinying Liang
 */

#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
//uses the same instance of th table class, avoid re-instantiating
Tables *SQLExec::tables = nullptr;



// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres)
{
  if (qres.column_names != nullptr)
  {
    for (auto const &column_name : *qres.column_names)
      out << column_name << " ";
    out << endl
        << "+";
    for (unsigned int i = 0; i < qres.column_names->size(); i++)
      out << "----------+";
    out << endl;
    for (auto const &row : *qres.rows)
    {
      for (auto const &column_name : *qres.column_names)
      {
        Value value = row->at(column_name);
        switch (value.data_type)
        {
        case ColumnAttribute::INT:
          out << value.n;
          break;
        case ColumnAttribute::TEXT:
          out << "\"" << value.s << "\"";
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

/**
 *Destructor
 */
QueryResult::~QueryResult()
{
  if (column_names != nullptr)
    delete column_names;
  if (column_attributes != nullptr)
    delete column_attributes;
  if (rows != nullptr)
  {
    for (auto row : *rows)
    {
      delete row;
    }

    delete rows;
  }
}

QueryResult *SQLExec::execute(const SQLStatement *statement)
{
  // initialize _tables table, if not yet present
  if (SQLExec::tables == nullptr)
  {
    SQLExec::tables = new Tables();
  }

  try
  {
    switch (statement->type())
    {
    case kStmtCreate:
      return create((const CreateStatement *)statement);
    case kStmtDrop:
      return drop((const DropStatement *)statement);
    case kStmtShow:
      return show((const ShowStatement *)statement);
    default:
      return new QueryResult("not implemented");
    }
  }
  catch (DbRelationError &e)
  {
    throw SQLExecError(string("DbRelationError: ") + e.what());
  }
}

/**
 *define column name and column attribute data type (datatype be one of INT or TEXT for now)
 *@param: ColumnDefinition, Identifier, ColumnAttribute
 *@return none
 */
void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute)
{
  column_name = col->name;

  switch (col->type)
  {
  case ColumnDefinition::INT:
    column_attribute.set_data_type(ColumnAttribute::INT);
    break;

  case ColumnDefinition::TEXT:
    column_attribute.set_data_type(ColumnAttribute::TEXT);
    break;

  case ColumnDefinition::DOUBLE: //do nothing for now, fallthrough into default

  default:
    throw SQLExecError("unrecognized data type");
  }
}


/**
 *Exceution of create_table
 */
QueryResult *SQLExec::create(const CreateStatement *statement)
{

  switch (statement->type) {
  case CreateStatement::kTable:
    return create_table(statement);
  case CreateStatement::kIndex:
    return create_index(statement);
  default:
    return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
  }
  
}

/**
 *Create table with given sql statements
 *@param: sql statement
 *@return: sql create query result
 *Fixed based on Kevin's provided code
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement)
{

  Identifier table_name = statement->tableName;
  ColumnNames column_names;
  ColumnAttributes column_attributes;
  Identifier column_name;
  ColumnAttribute column_attribute;
  
  for (ColumnDefinition *col : *statement->columns) {
    column_definition(col, column_name, column_attribute);
    column_names.push_back(column_name);
    column_attributes.push_back(column_attribute);
  }

  // Add to schema: _tables and _columns
  ValueDict row;
  row["table_name"] = table_name;
  Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
  try {
    Handles c_handles;
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    try {
      for (uint i = 0; i < column_names.size(); i++) {
        row["column_name"] = column_names[i];
        row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
        c_handles.push_back(columns.insert(&row));  // Insert into _columns
      }
      
      //create the relation
      DbRelation &table = SQLExec::tables->get_table(table_name);
      if (statement->ifNotExists)
        table.create_if_not_exists();
      else
        table.create();
      
    } catch (exception &e) {
      try {
        for (auto const &handle: c_handles)
          columns.del(handle);
      } catch (...) {}
      throw;
    }

  } catch (exception &e) {
    try {
      SQLExec::tables->del(t_handle);
    } catch (...) {}
    throw;
  }
  return new QueryResult("CREATED " + table_name);
  
}

/**
 *Milestone 4
 *create_index
 *Create an index with given table_name and columns
 */

QueryResult *SQLExec::create_index(const CreateStatement *statement)
{

  return new QueryResult("Not yet implemented"); //Todo
}

/**
 *Execution of drop_table
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
  switch (statement->type) {
  case DropStatement::kTable:
    return drop_table(statement);
  case DropStatement::kIndex:
    return drop_index(statement);
  default:
    return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
  }
}

/*
 *Drop table
 *@param: sql drop statement
 *@return: sql DROP query result
 *Fixed based on Kevin's codes
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement)
{
  Identifier table_name = statement->name;
  if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
    throw SQLExecError("cannot drop a schema table");
  
  ValueDict where;
  where["table_name"] = Value(table_name);
  
  // get the table
  DbRelation &table = SQLExec::tables->get_table(table_name);
  
  // remove from _columns schema
  DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
  Handles *handles = columns.select(&where);
  for (auto const &handle: *handles)
    columns.del(handle);
  delete handles;

  // remove table
  table.drop();
  
  //remove from _tables schema
  handles = SQLExec::tables->select(&where);
  SQLExec::tables->del(*handles->begin()); 
  delete handles;
    
    
    
  return new QueryResult(string("DROPPED ") + table_name);
}

/**
 *Milestone 4
 *drop_index
 */

QueryResult *SQLExec::drop_index(const DropStatement *statement)
{

  return new QueryResult("drop_index not yet implemented."); //TODO: MS4

}

QueryResult *SQLExec::show(const ShowStatement *statement)
{
  switch (statement->type)
  {
  case ShowStatement::kTables:
    return show_tables();
  case ShowStatement::kColumns:
    return show_columns(statement);
  default:
    throw SQLExecError("Unrecognized SHOW type");
  }
}

QueryResult *SQLExec::show_tables()
{
  ColumnNames *column_names = new ColumnNames();
  column_names->push_back("table_name");

  ColumnAttributes *column_attributes = new ColumnAttributes();
  column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

  ValueDicts *rows = new ValueDicts;
  Handles *handles = SQLExec::tables->select();

  for (auto const &handle : *handles)
  {
    ValueDict *row = SQLExec::tables->project(handle, column_names);
    Identifier table_name = (*row)["table_name"].s;
    if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
    {
      rows->push_back(row);
    }
    else
    {
      delete row;
    }
  }

  delete handles;

  return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}


/**
 *Milestone 4
 *Show index
 */
QueryResult *SQLExec::show_index(const ShowStatement *statement)
{
  return new QueryResult("not implemented"); //TODO

}

QueryResult *SQLExec::show_columns(const ShowStatement *statement)
{
  DbRelation &table = SQLExec::tables->get_table(Columns::TABLE_NAME);

  ColumnNames *column_names = new ColumnNames();
  ColumnAttributes *column_attributes = new ColumnAttributes();
  ValueDicts *rows = new ValueDicts;
  ValueDict results;

  column_names->push_back("table_name");
  column_names->push_back("column_name");
  column_names->push_back("data_type");

  column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

  results["table_name"] = Value(statement->tableName);
  Handles *handles = table.select(&results);
  

  for (auto const &handles : *handles)
  {
    ValueDict *row = table.project(handles, column_names);
    rows->push_back(row);
  }

  delete handles;

  return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}
