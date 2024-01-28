// Shell to execute SQL commands
// Dominic Burgi
// CPSC 5300 - Butterfly

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sql_exec.h"

// #define DEBUG_ENABLED
#include "debug.h"

using namespace std;
using namespace hsql;

// Constants
string const USAGE = "Program requires 1 argument, the path to a writeable directory\n";
string const EXIT_STR = "quit";
char const * DB_NAME = "butterfly.db";
unsigned int const BLOCK_SZ = 100;

SqlExec::SqlExec(): cout_buf(cout.rdbuf()) {}

void SqlExec::Execute(const SQLStatement* stmt) {
    switch (stmt->type())
    {
    case kStmtSelect:   PrintSelectStatementInfo((const SelectStatement*)stmt); break;
    case kStmtCreate:   PrintCreateStatementInfo((const CreateStatement*)stmt); break;
    default:            printf("Nothing to do for this statement...\n");        break;
    }
}

void SqlExec::PrintSelectStatementInfo(const SelectStatement* stmt)
{
    string ret("SELECT");
    ret += SelectListToString(stmt->selectList);
    ret += " FROM";
    ret += TableRefToString(stmt->fromTable);
    if (stmt->whereClause != NULL)
    {
        ret += " WHERE";
        ret += ExprToString(stmt->whereClause);
    }

    printf("%s\n", ret.c_str());
}

string SqlExec::TableRefToString(TableRef* tableRef)
{
    string ret;
    if (tableRef->join != NULL)
    {
        DEBUG_OUT("It has a join\n");
        ret += JoinDefToString(tableRef->join);
    }

    if (tableRef->name != NULL)
    {
        ret += " ";
        DEBUG_OUT("It has a non-null name...\n");
        if (tableRef->alias != NULL)
        {
            ret += tableRef->name;
            ret += " AS ";
        }
        ret += tableRef->getName();
    }

    if (tableRef->type == TableRefType::kTableCrossProduct)
    {
        for (size_t i = 0; i < tableRef->list->size(); i++)
        {
            ret += TableRefToString((*tableRef->list)[i]);
            if (i < tableRef->list->size() - 1)
            {
                ret += ",";
            }
        }
    }
    return ret;
}

string SqlExec::JoinDefToString(JoinDefinition* joinDef)
{
    string ret(" ");
    if (joinDef->left->alias != NULL)
    {
        ret += joinDef->left->name;
        ret += " AS ";
    }
    ret += joinDef->left->getName();
    ret += JoinTypeToString(joinDef->type);
    ret += " ";

    if (joinDef->right->alias != NULL)
    {
        ret += joinDef->right->name;
        ret += " AS ";
    }
    ret += joinDef->right->getName();
    ret += " ON";
    ret += ExprToString(joinDef->condition);
    return ret;
}

string SqlExec::JoinTypeToString(JoinType type)
{
    string ret;
    switch(type)
    {
    case JoinType::kJoinInner:      /* ret = " INNER"; */   break;
    case JoinType::kJoinOuter:      ret = " OUTER";         break;
    case JoinType::kJoinLeft:       ret = " LEFT";          break;
    case JoinType::kJoinRight:      ret = " RIGHT";         break;
    case JoinType::kJoinLeftOuter:  ret = " LEFT OUTER";    break;
    case JoinType::kJoinRightOuter: ret = " RIGHT OUTER";   break;
    case JoinType::kJoinCross:      ret = " CROSS";         break;
    case JoinType::kJoinNatural:    ret = " NATURAL";       break;
    default:                        ret = "...";            break;
    }
    ret += " JOIN";
    return ret;
}

string SqlExec::SelectListToString(vector<Expr*>* selectList)
{
    string ret;
    for (size_t i = 0; i < selectList->size(); i++)
    {
        ret += ExprToString((*selectList)[i]);
        if (i < selectList->size() - 1)
        {
            ret += ",";
        }
    }
    return ret;
}

string SqlExec::ExprToString(Expr* expr)
{
    DEBUG_OUT_VAR("Expr type?: %d\n", expr->type);
    string ret(" ");
    switch(expr->type)
    {
    case ExprType::kExprLiteralInt:
        ret += to_string(expr->ival);
        break;
    case ExprType::kExprStar:
        ret += "*";
        break;
    case ExprType::kExprColumnRef:
        if (expr->table != NULL)
        {
            ret += expr->table;
            ret += ".";
        }
        ret += expr->name;
        break;
    case ExprType::kExprOperator:
        ret = OpToString(expr);
        break;
    default:
        ret = "...";
        break;
    }
    return ret;
}

string SqlExec::OpToString(Expr* op)
{
    DEBUG_OUT_VAR("OpToString type: %d\n", op->opType);
    string ret;
    switch(op->opType)
    {
    case Expr::OperatorType::SIMPLE_OP:
        ret += ExprToString(op->expr);
        ret += " ";
        ret += op->opChar;
        ret += ExprToString(op->expr2);
        break;
    default:
        ret += "...";
    }
    return ret;
}

void SqlExec::PrintCreateStatementInfo(const CreateStatement* stmt)
{
    string ret("CREATE");
    ret += CreateTypeToString(stmt->type);
    if (stmt->type != CreateStatement::CreateType::kTable)
    {
        ret += "...";
    }
    else
    {
        ret += " ";
        ret += stmt->tableName;
        ret += " (";
        for (size_t i = 0; i < stmt->columns->size(); i++)
        {
            ret += ColumnDefinitionToString((*stmt->columns)[i]);
            if (i < stmt->columns->size() - 1)
            {
                ret += ", ";
            }
        }
        ret += ")";
    }

    printf("%s\n", ret.c_str());
}

string SqlExec::CreateTypeToString(const CreateStatement::CreateType type)
{
    string ret(" ");
    switch(type)
    {
    case CreateStatement::CreateType::kTable:           ret += "TABLE";         break;
    case CreateStatement::CreateType::kTableFromTbl:    ret += "TABLE FROM";    break;
    case CreateStatement::CreateType::kView:            ret += "VIEW";          break;
    case CreateStatement::CreateType::kIndex:           ret += "INDEX";         break;
    default:                                            ret += "...";           break;
    }
    return ret;
}


string SqlExec::ColumnDefinitionToString(const ColumnDefinition *col)
{
    string ret(col->name);
    switch(col->type)
    {
    case ColumnDefinition::DOUBLE:  ret += " DOUBLE";   break;
    case ColumnDefinition::INT:     ret += " INT";      break;
    case ColumnDefinition::TEXT:    ret += " TEXT";     break;
    default:                        ret += " ...";      break;
    }
    return ret;
}