// Header for SQL shell
// Dominic Burgi
// CPSC 5300 - Butterfly

#ifndef SQL_SHELL_H
#define SQL_SHELL_H

#include "db_cxx.h"
#include "SQLParserResult.h"
#include "sql/SQLStatement.h"
#include "sql/CreateStatement.h"
#include "sql/SelectStatement.h"

using namespace std;
using namespace hsql;

class SqlShell
{
public:
    SqlShell();
    void InitializeDbEnv(string envdir);
    void Run();
private:
    void Execute(SQLParserResult* result);
    void PrintStatementInfo(const SQLStatement* stmt);
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
    bool initialized;
    DbEnv env;
    Db db;


};

#endif // SQL_SHELL_H