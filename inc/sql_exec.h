// Header for SQL shell
/**
 * @file sql_exec.h - Implementaion of a SQL executor
 * SqlExec
 *
 * @author Kevin Lundeen, Dominic Burgi
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#pragma once

#include "db_cxx.h"
#include "SQLParserResult.h"
#include "sql/SQLStatement.h"
#include "sql/CreateStatement.h"
#include "sql/SelectStatement.h"

using namespace std;
using namespace hsql;

class SqlExec {
public:
    SqlExec();
    void Execute(const SQLStatement* stmt);
private:
    streambuf * cout_buf;

    void PrintSelectStatementInfo(const SelectStatement* stmt);
    
    void PrintCreateStatementInfo(const CreateStatement* stmt);

    string CreateTypeToString(const CreateStatement::CreateType type);

    string ColumnDefinitionToString(const ColumnDefinition *col);

    string ExprToString(Expr* expr);

    string SelectListToString(vector<Expr*>* selectList);

    string TableRefToString(TableRef* tableRef);

    string JoinDefToString(JoinDefinition* joinDef);

    string JoinTypeToString(JoinType type);

    string OpToString(Expr* op);

    string WhereClauseToString(Expr* where);

};