// Shell to execute SQL commands
// Dominic Burgi
// CPSC 5300 - Butterfly

#include <iostream>     // std::streambuf, std::cout
#include <fstream>      // std::ofstream
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sql/SQLStatement.h"
#include "sql/SelectStatement.h"
#include "sql/CreateStatement.h"

#include "sql_shell.h"

#define DEBUG_ENABLED
#include "debug.h"

const char *DB_NAME = "butterfly.db";
const unsigned int BLOCK_SZ = 100;
const std::string EXIT_STR = "quit";

SqlShell::SqlShell()
    : cout_buf(std::cout.rdbuf())
    , initialized(false)
    , env(0U)
    , db(&env, 0)
{}

void SqlShell::InitializeDbEnv(std::string envdir)
{
    DEBUG_OUT("Start of SqlShell::InitializeDbEnv()\n");
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ); // Set record length
    db.open(NULL, DB_NAME, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

    initialized = true;
}

void SqlShell::Run()
{
    DEBUG_OUT("Start of SqlShell::Run()\n");
    if(!initialized)
    {
        DEBUG_OUT("DB env must be initialized before running the shell\n");
        return;
    }

    std::string sql;
    std::string canonical_sql;
    hsql::SQLParserResult* parse;

    DEBUG_OUT("Entering SQL processing loop\n");
    printf("Type \"%s\" to exit.\n", EXIT_STR.c_str());
    while(1)
    {
        printf("SQL> ");
        getline(std::cin, sql);
        if (sql == EXIT_STR)
        {
            break;
        }

        parse = hsql::SQLParser::parseSQLString(sql);

        DEBUG_OUT("Checking validity...\n");
        if(!parse->isValid())
        {
            printf("Invalid SQL: %s\n", sql.c_str());
            continue;
        }

        DEBUG_OUT("Statement valid, executing...\n");
        Execute(parse);
    }

}


// Execute the SQL statement from the given parse tree.
void SqlShell::Execute(hsql::SQLParserResult* parse)
{
    DEBUG_OUT("SQL recognized, executing\n");

    for (auto i = 0; i < parse->size(); i++) {
        PrintStatementInfo(parse->getStatement(i));
    }
}

void SqlShell::PrintStatementInfo(const hsql::SQLStatement* stmt) {
    switch (stmt->type()) {
        case hsql::kStmtSelect:
            PrintSelectStatementInfo((const hsql::SelectStatement*)stmt);
            break;
        // case kStmtInsert:
        //     printInsertStatementInfo((const InsertStatement*)stmt, 0);
        //     break;
        case hsql::kStmtCreate:
            PrintCreateStatementInfo((const hsql::CreateStatement*)stmt);
            break;
        // case kStmtImport:
        //     printImportStatementInfo((const ImportStatement*)stmt, 0);
        //     break;
        // case kStmtExport:
        //     printExportStatementInfo((const ExportStatement*)stmt, 0);
        //     break;
        // case kStmtTransaction:
        //     printTransactionStatementInfo((const TransactionStatement*)stmt, 0);
        //     break;
        default:
            printf("Nothing to do for this statement type yet...\n");
            break;
    }
}

void SqlShell::PrintSelectStatementInfo(const hsql::SelectStatement* stmt)
{
    std::string ret("SELECT");
    printf("%s\n", ret.c_str());
}

void SqlShell::PrintCreateStatementInfo(const hsql::CreateStatement* stmt)
{
    std::string ret("CREATE");
    ret += CreateTypeToString(stmt->type);
    if (stmt->type != hsql::CreateStatement::CreateType::kTable)
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

std::string SqlShell::CreateTypeToString(const hsql::CreateStatement::CreateType type)
{
    std::string ret;
    switch(type) {
    case hsql::CreateStatement::CreateType::kTable:
        ret = " TABLE";
        break;
    case hsql::CreateStatement::CreateType::kTableFromTbl:
        ret = " TABLE FROM";
        break;
    case hsql::CreateStatement::CreateType::kView:
        ret = " VIEW";
        break;
    case hsql::CreateStatement::CreateType::kIndex:
        ret = " INDEX";
        break;
    }
    return ret;
}


std::string SqlShell::ColumnDefinitionToString(const hsql::ColumnDefinition *col)
{
    std::string ret(col->name);
    switch(col->type) {
    case hsql::ColumnDefinition::DOUBLE:
        ret += " DOUBLE";
        break;
    case hsql::ColumnDefinition::INT:
        ret += " INT";
        break;
    case hsql::ColumnDefinition::TEXT:
        ret += " TEXT";
        break;
    default:
        ret += " ...";
        break;
    }
    return ret;
}