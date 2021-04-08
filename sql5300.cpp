//
// Created by jarak on 4/6/2021.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <algorithm>
#include "sql5300.h"
#include <sqlhelper.h>
#include <SQLParser.h>
#include <db_cxx.h>
#include <string>

const char QUIT[5] = "quit";
const char *HOME = "cpsc5300/data";
const char *SQLSHELL = "sql5300.db";
const unsigned int BLOCK_SZ = 4096;


sql5300::sql5300() {

}

//TODO: Fix shell.execute() by adding query parameter (AST type?)
//TODO: Convert AST to string
//SQLParserResult*
std::string sql5300::execute() {

    // Test
    std::cout << "\nGot to execute\n" << std::endl;





    // Delete
    std::string hello = "Hello";
    return hello;

}

int main( int argc, char *argv[]) {

    sql5300 shell;
    std::string query = argv[1];

    // TODO: check if this is correct for creating th db?
    //Create DB environment
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


    // User prompt loop
    while (true) {

        //Parse response
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);

        // Convert query to lowercase for quit
        std::string lowercase = query;
        transform(lowercase.begin(), lowercase.end(), lowercase.begin(), ::tolower);

        // Exit if user enters quit (any case).
        if (lowercase == QUIT) {
            std::cout << "\nExiting Program\n" << std::endl;
            break;
        }

        //Valid response
        if (result->isValid()) {
            printf("Valid");
            //TODO: Fix shell.execute() by adding query parameter (AST type?)
            shell.execute();
            delete result;


        } else { //Invalid response
            fprintf(stderr, "Given string is not valid.\n");
            fprintf(stderr, "%s (L%d:%d)\n", result->errorMsg(),
                    result->errorLine(), result->errorColumn());
            delete result;
        }

        // Take in new query
        std::cout << "SQL> ";
        char newquery[200];
        std::cin.getline(newquery,200);
        query = newquery;
        std::cout << newquery<< std::endl;

    }

    return 0;

}


