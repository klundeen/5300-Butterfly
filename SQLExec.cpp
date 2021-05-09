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
 *Create table
 *@param: sql statement
 *@return: sql create query result
 */
QueryResult *SQLExec::create(const CreateStatement *statement)
{

  if (statement->type != CreateStatement::kTable || statement->type != CreateStatement::kIndex)
    return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");

  Identifier table_name = statement->tableName;
  ColumnNames column_names; //set of col names
  ColumnAttributes column_attributes;
  Identifier column_name;           //one column name
  ColumnAttribute column_attribute; //one column attribute

  for (ColumnDefinition *col : *statement->columns)
  {
    column_definition(col, column_name, column_attribute);
    column_names.push_back(column_name);
    column_attributes.push_back(column_attribute);
  }

  // insert table and column to schema
  ValueDict row;
  row["table_name"] = table_name;

  //insert into _tables
  Handle table_handle = SQLExec::tables->insert(&row);

  try
  {
    Handles col_handles;
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    try
    {
      for (uint i = 0; i < column_names.size(); i++)
      {
        row["column_name"] = column_names[i];
        row["date_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
        col_handles.push_back(columns.insert(&row));
      }

      //Create the relation
      DbRelation &table = SQLExec::tables->get_table(table_name);
      if (statement->ifNotExists)
        table.create_if_not_exists();
      else
        table.create();
    }
    catch (exception &e)
    {
      //attemp to undo the insertions into _columns
      try
      {
        for (auto const &handle : col_handles)
          columns.del(handle);
      }
      catch (...)
      {
      }
      throw;
    }
  }
  catch (exception &e)
  {
    //attemp to remove undo the insertions into _tables
    try
    {
      SQLExec::tables->del(table_handle);
    }
    catch (...)
    {
    }
    throw;
  }

  return new QueryResult("CREATED " + table_name);
}

/**
 *Milestone 4
 *create_index
 */

QueryResult *SQLExec::create_index(const CreateStatement *statement)
{

  return new QueryResult("create_index not yet implemented"); //TODO: MS4
  
}

/**
 *Drop table
 *@param: sql drop statement
 *@return: sql DROP query result
 */
QueryResult *SQLExec::drop(const DropStatement *statement)
{

  if (statement->type != DropStatement::kTable || statement->type != DropStatement::kIndex)
    return new QueryResult("Only DROP TABLE and DROP INDEX are implemented.");

  Identifier table_name = statement->name;
  if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
    throw SQLExecError("Unable to drop a schema table");

  ValueDict where;
  where["table_name"] = Value(table_name);

  //get the table
  DbRelation &table = SQLExec::tables->get_table(table_name);

  DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
  Handles *col_handles = columns.select(&where);
  for (auto const &handle : *col_handles)
    columns.del(handle);

  delete col_handles;

  //drop the table before removing from the schema
  table.drop();

  //remove from _tables schema
  SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

  return new QueryResult("DROPPED " + table_name);
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
    if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME)
    {
      rows->push_back(row);
    }
    else
    {
      delete row;
    }
  }

  delete handles;

  return new QueryResult(column_names, column_attributes, rows, "successfully returned" + to_string(rows->size()) + " rows");
}


/**
 *Milestone 4
 *Show index
 */
QueryResult *SQLExec::show_index(const ShowStatment *statement)
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
