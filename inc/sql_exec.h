// Header for SQL shell
// Dominic Burgi
// CPSC 5300 - Butterfly

#ifndef SQL_EXEC_H
#define SQL_EXEC_H

#include "db_cxx.h"
#include "SQLParserResult.h"
#include "sql/SQLStatement.h"
#include "sql/CreateStatement.h"
#include "sql/SelectStatement.h"

using namespace std;
using namespace hsql;

class SqlExec
{
public:
    SqlExec();
    void Execute(const SQLStatement* stmt);
private:
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
    streambuf * cout_buf;
};

#endif // SQL_EXEC_H