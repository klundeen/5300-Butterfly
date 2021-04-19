/**
 * CPSC-5300 Butterfly ,Sprint Verano:
 * @file sql5300.cpp - Main entry for the relation manager's SQL shell
 * @author Shrividya Ballapadavu, Jara Lindsay
 * @author Kevin Lundeen, Supplied starting components
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstring>
#include <iostream>
#include <algorithm>
#include "sql5300.h"
#include "sqlhelper.h"
#include "SQLParser.h"
#include "heap_storage.h"
#include "db_cxx.h"
#include <string>

const char QUIT[5] = "quit";

/*
 * allocate and initialize the _DB_ENV global
 */
DbEnv *_DB_ENV;

std::string convertExpressionToString(hsql::Expr *expr, std::string res);

std::string convertSelectStatementInfo(const hsql::SelectStatement *stmt, std::string res);

/**
 * Convert the hyrise OperatorExpression AST back into the equivalent SQL String
 * @param expr expression to unparse
 * @param res SQL String to add expression to
 * @return formatted SQL String
 */
std::string convertOperatorExpressionToString(hsql::Expr *expr, std::string res) {
    if (expr == NULL) {

        return res;
    }
    res = convertExpressionToString(expr->expr, res);
    switch (expr->opType) {
        case hsql::Expr::SIMPLE_OP:
            res += std::string(" ") + (char) expr->opChar;
            break;
        case hsql::Expr::AND:
            res += std::string(" AND");
            break;
        case hsql::Expr::OR:
            res += std::string(" OR");
            break;
        case hsql::Expr::NOT:
            res += std::string(" NOT");
            break;
        default:
            res += std::string(" ") + (char) expr->opType;
            break;
    }

    if (expr->expr2 != NULL) {
        res = convertExpressionToString(expr->expr2, res);
    }

    return res;
}

/**
 * Convert the hyrise Expression AST back into the equivalent SQL String
 * @param expr expression to unparse
 * @param res SQL String to add expression to
 * @return formatted SQL String
 */
std::string convertExpressionToString(hsql::Expr *expr, std::string res) {
    switch (expr->type) {
        case hsql::kExprStar:
            res += " *";
            break;
        case hsql::kExprColumnRef:
            if (expr->table) {
                res += std::string(" ") + expr->table + "." + expr->name;
            } else {
                res += std::string(" ") + expr->name;
            }
            break;

        case hsql::kExprLiteralFloat:
            res += std::string(" ") + std::to_string(expr->fval) + " ";
            break;
        case hsql::kExprLiteralInt:
            res += std::string(" ") + std::to_string(expr->ival) + " ";
            break;
        case hsql::kExprLiteralString:
            res += std::string(" ") + expr->name;
            break;
        case hsql::kExprFunctionRef:
            res += std::string(" ") + expr->name + " " + expr->expr->name + " ";
            break;
        case hsql::kExprOperator:
            res = convertOperatorExpressionToString(expr, res);
            break;
        default:
            fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
            break;
    }

    if (expr->alias != NULL) {
        std::cout << " inside alias ";
        res += std::string(" ") + expr->alias + "." + expr->name + " ";

    }
    return res;
}

/**
 * Convert the hyrise TableRefInfo AST back into the equivalent SQL String
 * @param table hyrise table pointer
 * @param res SQL String to add expression to
 * @return formatted SQL String
 */
std::string convertTableRefInfoToString(hsql::TableRef *table, std::string res) {
    switch (table->type) {
        case hsql::kTableName:

            res += std::string(" ") + table->name;


            break;

        case hsql::kTableSelect:
            res = convertSelectStatementInfo(table->select, res);
            break;

        case hsql::kTableJoin:
            res = convertTableRefInfoToString(table->join->left, res);
            switch (table->join->type) {
                case hsql::kJoinInner:
                    res += " JOIN";
                    break;
                case hsql::kJoinLeft:
                    res += " LEFT JOIN";
                    break;
                case hsql::kJoinRight:
                    res += " RIGHT JOIN";
                    break;
                default:
                    break;
            }

            res = convertTableRefInfoToString(table->join->right, res);
            res += " ON";
            res = convertExpressionToString(table->join->condition, res);
            break;

        case hsql::kTableCrossProduct:
            for (hsql::TableRef *tbl : *table->list)
                res = convertTableRefInfoToString(tbl, res);
            break;
    }
    if (table->alias != NULL) {
        res += std::string(" AS ") + table->alias;
    }

    return res;
}

/**
 * Convert the hyrise SelectStatement AST back into the equivalent SQL
 * @param stmt select statement to unparse
 * @param res SQL String to add expression to
 * @return formatted SQL String
 */
std::string convertSelectStatementInfo(const hsql::SelectStatement *stmt, std::string res) {
    res += " SELECT";
    bool doComma = false;
    for (hsql::Expr *expr : *stmt->selectList) {
        if (doComma) {
            res += ",";
        }
        res = convertExpressionToString(expr, res);
        doComma = true;
    }

    res += " FROM";
    res = convertTableRefInfoToString(stmt->fromTable, res);
    if (stmt->whereClause != NULL) {
        res += " WHERE";
        res = convertExpressionToString(stmt->whereClause, res);
    }

    return res;

}


/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
std::string columnDefinitionToString(const hsql::ColumnDefinition *col) {
    std::string ret(col->name);
    switch (col->type) {
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

/**
 * Convert the hyrise CreateStatement AST back into the equivalent SQL
 * @param stmt create statement to unparse
 * @param res SQL String to add expression to
 * @return formatted SQL String
 */
std::string convertCreateStatementInfo(const hsql::CreateStatement *stmt, std::string res) {
    res += "CREATE TABLE ";
    res += std::string("") + stmt->tableName;


    if (stmt->columns != NULL) {
        res += " (";
        bool doComma = false;
        for (auto col_name:*stmt->columns) {
            if (doComma) {
                res += ", ";
            }
            res += std::string("") + columnDefinitionToString(col_name);
            doComma = true;

        }
        res += ")";
    }


    return res;
}

/**
 * Convert the hyrise SQLStatement AST back into the equivalent SQL
 * @param stmt SQL statement to unparse
 * @param res SQL String to add expression to
 * @return formatted SQL String
 */
std::string execute(const hsql::SQLStatement *stmt, std::string res) {

    switch (stmt->type()) {
        case hsql::kStmtSelect:
            res = convertSelectStatementInfo((const hsql::SelectStatement *) stmt, res);
            break;
        case hsql::kStmtCreate:
            res = convertCreateStatementInfo((const hsql::CreateStatement *) stmt, res);
            break;
        default:
            break;


    }


    return res;

}

/**
 * Main entry point for the program. Creates a DB environment and prompts the user
 * for a series of SQL queries. User entries are validated, parsed, and printed as
 * output. User can terminate the program by entering 'quit'.
 * @return 0 exit program
 */
int main(int argc, char **argv) {

    //Create DB environment
    char *home = argv[1];

    if (argc != 2) {
        std::cerr << "Example usage: ./sql5300 ~/cpsc5300/data" << std::endl;
        return 1;
    }

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    try {
        env.open(home, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        std::cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }

    // Database environment success message
    std::cout << "(sql5300: Running with database environment at " << home << ")" << std::endl;

    _DB_ENV = &env;

    char newquery[200];
    // User prompt loop
    while (true) {
        // Take in new query
        std::cout << "SQL> ";
        std::cin.getline(newquery, 200);
        std::string query = newquery;

        // Convert query to lowercase for quit
        std::string lowercase = query;
        transform(lowercase.begin(), lowercase.end(), lowercase.begin(), ::tolower);
        if (query.length() == 0)
            continue;  // skip if user enters blank line

        // Exit if user enters quit (any case).
        if (lowercase == QUIT) {
            std::cout << "\nExiting Program\n" << std::endl;
            break;
        }
        if (query == "test") {
            std::cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << std::endl;
            continue;
        }

        //Parse response
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);

        //Valid response
        if (result->isValid()) {
            std::string res;
            for (uint i = 0; i < result->size(); ++i) {
                res += execute(result->getStatement(i), res);
            }

            std::cout << res << std::endl;
            delete result;


        } else { //Invalid response
            fprintf(stderr, "Invalid SQL: ");
            std::cout << query << std::endl;
            fprintf(stderr, "%s (L%d:%d)\n", result->errorMsg(),
                    result->errorLine(), result->errorColumn());
            delete result;
        }

    }

    return 0;

}


