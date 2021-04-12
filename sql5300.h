//
// Created by jarak on 4/6/2021.
//

#ifndef INC_5300_BUTTERFLY_SQL5300_H
#define INC_5300_BUTTERFLY_SQL5300_H

#include "sqlhelper.h"
#include "SQLParser.h"

class sql5300 {
public:

    // Constructor
    sql5300() = default;

    // Destructor
    ~sql5300();

    // Convert the hyrise Expression AST back into the equivalent SQL String
    std::string convertExpressionToString(hsql::Expr *expr, std::string res);

    // Convert the hyrise SelectStatement AST back into the equivalent SQL
    std::string convertSelectStatementInfo(const hsql::SelectStatement *stmt, std::string res);

    // Convert the hyrise OperatorExpression AST back into the equivalent SQL String
    std::string convertOperatorExpressionToString(hsql::Expr *expr, std::string res);

    // Convert the hyrise TableRefInfo AST back into the equivalent SQL String
    std::string convertTableRefInfoToString(hsql::TableRef *table, std::string res);

    // Convert the hyrise ColumnDefinition AST back into the equivalent SQL
    std::string columnDefinitionToString(const hsql::ColumnDefinition *col);

    // Convert the hyrise CreateStatement AST back into the equivalent SQL
    std::string convertCreateStatementInfo(const hsql::CreateStatement *stmt, std::string res);

    // Execute function: Convert the hyrise SQLStatement AST back into the equivalent SQL
    std::string execute(const hsql::SQLStatement *stmt, std::string res);

};


#endif //INC_5300_BUTTERFLY_SQL5300_H
