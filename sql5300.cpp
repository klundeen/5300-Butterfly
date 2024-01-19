// Shell to execute SQL commands
// Dominic Burgi
// CPSC 5300 - Butterfly

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sql_shell.h"

#define DEBUG_ENABLED
#include "debug.h"

using namespace std;
using namespace hsql;

// Constants
string const USAGE = "Program requires 1 argument, the path to a writeable directory\n";
string const EXIT_STR = "quit";
char const * DB_NAME = "butterfly.db";
unsigned int const BLOCK_SZ = 100;

// Helper functions
static bool DataDirExists(string data_dir);
static void GetArgs(int argc, char *argv[], string &data_dir);

int main(int argc, char *argv[])
{
    DEBUG_OUT("Start of main()\n");
    string data_dir;
    GetArgs(argc, argv, data_dir);

    if(!DataDirExists(data_dir))
    {
        cout << "Data dir must exist... Exiting.\n" << endl;
        return -1;
    }

    string envdir = data_dir;
    DEBUG_OUT_VAR("Initializing DB environment in %s\n", envdir.c_str());

    DEBUG_OUT("Running SQL Shell\n");
    SqlShell sql_shell;
    sql_shell.InitializeDbEnv(envdir);
    sql_shell.Run();
    return 0;
}


static bool DataDirExists(string data_dir)
{
    cout << "Have you created a dir: " << data_dir  << "? (y/n) " << endl;
    string ans;
    getline(cin, ans);
    if( ans[0] != 'y')
    {
        return false;
    }
    return true;
}


static void GetArgs(int argc, char *argv[], string &data_dir) {
    if (argc <= 1 || argc >= 3) {
        cerr << USAGE;
        exit(1);
    }

    data_dir = argv[1];
}

SqlShell::SqlShell()
    : cout_buf(cout.rdbuf())
    , initialized(false)
    , env(0U)
    , db(&env, 0)
{}

void SqlShell::InitializeDbEnv(string envdir)
{
    DEBUG_OUT("Start of SqlShell::InitializeDbEnv()\n");
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
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

    string sql;
    string canonical_sql;
    SQLParserResult* parse;

    DEBUG_OUT("Entering SQL processing loop\n");
    printf("Type \"%s\" to exit.\n", EXIT_STR.c_str());
    while(1)
    {
        printf("SQL> ");
        getline(cin, sql);
        if (sql == EXIT_STR)
        {
            break;
        }

        parse = SQLParser::parseSQLString(sql);

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
void SqlShell::Execute(SQLParserResult* parse)
{
    DEBUG_OUT("SQL recognized, printing...\n");

    for (size_t i = 0; i < parse->size(); i++) {
        PrintStatementInfo(parse->getStatement(i));
    }
}

void SqlShell::PrintStatementInfo(const SQLStatement* stmt) {
    switch (stmt->type()) {
        case kStmtSelect:
            PrintSelectStatementInfo((const SelectStatement*)stmt);
            break;
        // case kStmtInsert:
        //     printInsertStatementInfo((const InsertStatement*)stmt, 0);
        //     break;
        case kStmtCreate:
            PrintCreateStatementInfo((const CreateStatement*)stmt);
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

void SqlShell::PrintSelectStatementInfo(const SelectStatement* stmt)
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

string SqlShell::TableRefToString(TableRef* tableRef)
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

    if (tableRef->hasSchema())
    {
        DEBUG_OUT("It has a schema\n");
    }

    DEBUG_OUT_VAR("It has a type: %d\n", tableRef->type);
    DEBUG_OUT_VAR("list null?: %d\n", tableRef->list == NULL);

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

    if (tableRef->alias != NULL)
    {
        DEBUG_OUT("It has a alias\n");
    }

    if (tableRef->schema != NULL)
    {
        DEBUG_OUT("It has a schema non null\n");
    }
    return ret;
}

string SqlShell::JoinDefToString(JoinDefinition* joinDef)
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

string SqlShell::JoinTypeToString(JoinType type)
{
    string ret;
    switch(type) {
    case JoinType::kJoinInner:
        // ret = " INNER";
        break;
    case JoinType::kJoinOuter:
        ret = " OUTER";
        break;
    case JoinType::kJoinLeft:
        ret = " LEFT";
        break;
    case JoinType::kJoinRight:
        ret = " RIGHT";
        break;
    case JoinType::kJoinLeftOuter:
        ret = " LEFT OUTER";
        break;
    case JoinType::kJoinRightOuter:
        ret = " RIGHT OUTER";
        break;
    case JoinType::kJoinCross:
        ret = " CROSS";
        break;
    case JoinType::kJoinNatural:
        ret = " NATURAL";
        break;
    default:
        ret = "...";
        break;
    }
    ret += " JOIN";
    return ret;
}

string SqlShell::SelectListToString(vector<Expr*>* selectList)
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

string SqlShell::ExprToString(Expr* expr)
{
    DEBUG_OUT_VAR("Expr type?: %d\n", expr->type);
    string ret(" ");
    switch(expr->type) {
    case ExprType::kExprLiteralFloat:
        ret = "float";
        break;
    case ExprType::kExprLiteralString:
        ret = "string";
        break;
    case ExprType::kExprLiteralInt:
        ret += to_string(expr->ival);
        break;
    case ExprType::kExprStar:
        ret += "*";
        break;
    case ExprType::kExprPlaceholder:
        ret = "placeholder";
        break;
    case ExprType::kExprColumnRef:
        if (expr->table != NULL)
        {
            ret += expr->table;
            ret += ".";
        }
        ret += expr->name;
        break;
    case ExprType::kExprFunctionRef:
        ret = "func";
        break;
    case ExprType::kExprOperator:
        ret = OpToString(expr);
        break;
    case ExprType::kExprSelect:
        ret = "select";
        break;
    case ExprType::kExprUsing:
        ret = "USING";
        break;
    default:
        ret = "...";
        break;
    }
    return ret;
}

string SqlShell::OpToString(Expr* op)
{
    DEBUG_OUT_VAR("OpToString type: %d\n", op->opType);
    string ret;
    switch(op->opType) {
    case Expr::OperatorType::NONE:
        ret = " NONE";
        break;
    case Expr::OperatorType::BETWEEN:
        ret = " BETWEEN";
        break;
    case Expr::OperatorType::CASE:
        ret = " CASE";
        break;
    case Expr::OperatorType::SIMPLE_OP:
        ret += ExprToString(op->expr);
        ret += " ";
        ret += op->opChar;
        ret += ExprToString(op->expr2);
        break;
    case Expr::OperatorType::NOT_EQUALS:
        ret = " NOT_EQUALS";
        break;
    case Expr::OperatorType::LESS_EQ:
        ret = " LESS_EQ";
        break;
    case Expr::OperatorType::GREATER_EQ:
        ret = " GREATER_EQ";
        break;
    case Expr::OperatorType::LIKE:
        ret = " LIKE";
        break;
    case Expr::OperatorType::NOT_LIKE:
        ret = " NOT_LIKE";
        break;
    case Expr::OperatorType::AND:
        ret = " AND";
        break;
    case Expr::OperatorType::OR:
        ret = " OR";
        break;
    case Expr::OperatorType::IN:
        ret = " IN";
        break;
    case Expr::OperatorType::NOT:
        ret = " NOT";
        break;
    case Expr::OperatorType::UMINUS:
        ret = " UMINUS";
        break;
    case Expr::OperatorType::ISNULL:
        ret = " ISNULL";
        break;
    case Expr::OperatorType::EXISTS:
        ret = " EXISTS";
        break;
    }
    return ret;
}

void SqlShell::PrintCreateStatementInfo(const CreateStatement* stmt)
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

string SqlShell::CreateTypeToString(const CreateStatement::CreateType type)
{
    string ret;
    switch(type) {
    case CreateStatement::CreateType::kTable:
        ret = " TABLE";
        break;
    case CreateStatement::CreateType::kTableFromTbl:
        ret = " TABLE FROM";
        break;
    case CreateStatement::CreateType::kView:
        ret = " VIEW";
        break;
    case CreateStatement::CreateType::kIndex:
        ret = " INDEX";
        break;
    }
    return ret;
}


string SqlShell::ColumnDefinitionToString(const ColumnDefinition *col)
{
    string ret(col->name);
    switch(col->type) {
    case ColumnDefinition::DOUBLE:
        ret += " DOUBLE";
        break;
    case ColumnDefinition::INT:
        ret += " INT";
        break;
    case ColumnDefinition::TEXT:
        ret += " TEXT";
        break;
    default:
        ret += " ...";
        break;
    }
    return ret;
}