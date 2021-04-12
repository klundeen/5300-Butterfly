//
// Created by jarak on 4/6/2021.
// CPSC-5300 Butterfly ,Sprint Verano: Shrividya Ballapadavu, Jara Lindsay
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <algorithm>
#include "sql5300.h"
#include "sqlhelper.h"
#include "SQLParser.h"
#include <db_cxx.h>
#include <string>

const char QUIT[5] = "quit";
const char *HOME = "cpsc5300/data";
const char *SQLSHELL = "sql5300.db";
const unsigned int BLOCK_SZ = 4096;

std::string convertExpressionToString(hsql::Expr *expr, std::string res);
std::string convertSelectStatementInfo(const hsql::SelectStatement *stmt, std::string res);
std::string convertOperatorExpressionToString(hsql::Expr *expr, std::string res) {
    if (expr == NULL) {

        return res;
    }

    switch (expr->opType) {
        case hsql::Expr::SIMPLE_OP:
            res += std::string(" ") + (char) expr->opChar + " ";
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
    res = convertExpressionToString(expr->expr, res);
    if (expr->expr2 != NULL) {
        res = convertExpressionToString(expr->expr2, res);
    }

    return res;
}

std::string convertExpressionToString(hsql::Expr *expr, std::string res) {
    switch (expr->type) {
        case hsql::kExprStar:
            res += "*";
            break;
        case hsql::kExprColumnRef:
            if(expr->table) {
                res += std::string(" ") + expr->table+"."+expr->name + ",";
            }
            else{
                res += std::string(" ") +expr->name + ",";
            }
            break;

        case hsql::kExprLiteralFloat:
            res += std::string(" ") + (char) expr->fval + " ";
            break;
        case hsql::kExprLiteralInt:
            res += std::string(" ") + (char) expr->ival + " ";
            break;
        case hsql::kExprLiteralString:
            res += std::string(" ") +expr->name + ",";
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
        std::cout<<" inside alias ";
        res += std::string(" ") +expr->alias+"."+expr->name + " ";

    }
    return res;
}

std::string convertTableRefInfoToString(hsql::TableRef *table, std::string res) {

    switch (table->type) {
        case hsql::kTableName:

            res += std::string(" ") + table->name + ",";


            break;

        case hsql::kTableSelect:
            res=convertSelectStatementInfo(table->select, res);
            break;

        case hsql::kTableJoin:
            res = convertTableRefInfoToString(table->join->left, res);
            if(table->join->left){
                res += " LEFT ";
            }

            res += " JOIN ";
            res = convertTableRefInfoToString(table->join->right, res);
            res += " ON ";
            res = convertExpressionToString(table->join->condition, res);
            break;

        case hsql::kTableCrossProduct:
            for (hsql::TableRef *tbl : *table->list)
                res = convertTableRefInfoToString(tbl, res);
            break;
    }
    if (table->alias != NULL) {
        res += std::string("AS ") + table->alias + " ";
    }

    return res;
}

std::string convertSelectStatementInfo(const hsql::SelectStatement *stmt, std::string res) {
    res += " SELECT";

    for (hsql::Expr *expr : *stmt->selectList) {
        res = convertExpressionToString(expr, res);
    }

    res += " FROM";
    res = convertTableRefInfoToString(stmt->fromTable, res);
    if (stmt->whereClause != NULL) {
        res += " WHERE ";
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


std::string convertCreateStatementInfo(const hsql::CreateStatement *stmt, std::string res) {
    res += "CREATE TABLE ";
    res += std::string("") + stmt->tableName;


    if (stmt->columns != NULL) {
        res += " (";
        for (auto col_name:*stmt->columns) {
            res += std::string("") + columnDefinitionToString(col_name);
            res += ",";


        }
        res += ")";
    }


    return res;
}

std::string execute(const hsql::SQLStatement *stmt, std::string res) {

    // Test
    std::cout << "\nGot to execute\n" << std::endl;
    // printf("Parsed successfully!\n");
    // printf("Number of statements: %lu\n", result->size());


    // Print a statement summary.
    // hsql::printStatementInfo(result->getStatement(i));
    // const hsql::SQLStatement* stmt=result->getStatement(i);

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

int main(int argc, char *argv[]) {

    // sql5300 shell;


    // TODO: check if this is correct for creating th db?
    //Create DB environment
    /*
    std::cout << "Have you created a dir: ~/" << HOME  << "? (y/n) " << std::endl;
    std::string ans;
    std::cin >> ans;
    if( ans[0] != 'y')
	return 1;
    */
    const char *home = std::getenv("HOME");
    std::string envdir = std::string(home) + "/" + HOME;

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_re_len(BLOCK_SZ);
    db.open(NULL, SQLSHELL, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);

    char newquery[200];
    // User prompt loop
    while (true) {
        // Take in new query
        std::cout << "SQL> ";
        std::cin.getline(newquery, 200);
        std::string query = newquery;
        std::cout << query << std::endl;

        // Convert query to lowercase for quit
        std::string lowercase = query;
        transform(lowercase.begin(), lowercase.end(), lowercase.begin(), ::tolower);

        // Exit if user enters quit (any case).
        if (lowercase == QUIT) {
            std::cout << "\nExiting Program\n" << std::endl;
            break;
        }

        //Parse response
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);

        //Valid response
        if (result->isValid()) {
            printf("Valid");
            std::string res;
            //TODO: Fix shell.execute() by adding query parameter (AST type?)
            for (uint i = 0; i < result->size(); ++i) {
                res += execute(result->getStatement(i), res);
            }

            // Print a statement summary.
            // hsql::printStatementInfo(result->getStatement(i));
            // res=execute(result,res);

            std::cout << res << std::endl;
            //std::string res=shell.execute(&result);
            delete result;


        } else { //Invalid response
            fprintf(stderr, "Given string is not valid.\n");
            fprintf(stderr, "%s (L%d:%d)\n", result->errorMsg(),
                    result->errorLine(), result->errorColumn());
            delete result;
        }

    }

    return 0;

}


