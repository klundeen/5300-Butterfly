// Header for SQL shell
// Dominic Burgi
// CPSC 5300 - Butterfly

#ifndef SQL_SHELL_H
#define SQL_SHELL_H

#include <iostream> // std::streambuf
#include <fstream>  // std::ofstream
#include "db_cxx.h"
#include "SQLParserResult.h"
#include "sql/SQLStatement.h"
#include "sql/CreateStatement.h"
#include "sql/SelectStatement.h"

class SqlShell
{
public:
    SqlShell();
    void InitializeDbEnv(std::string envdir);
    void Run();
private:
    void Execute(hsql::SQLParserResult* result);
    void PrintStatementInfo(const hsql::SQLStatement* stmt);
    void PrintSelectStatementInfo(const hsql::SelectStatement* stmt);
    void PrintCreateStatementInfo(const hsql::CreateStatement* stmt);
    std::string CreateTypeToString(const hsql::CreateStatement::CreateType type);
    std::string ColumnDefinitionToString(const hsql::ColumnDefinition *col);
    std::streambuf * cout_buf;
    bool initialized;
    DbEnv env;
    Db db;


};

#endif // SQL_SHELL_H