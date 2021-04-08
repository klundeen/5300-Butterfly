//
// Created by jarak on 4/6/2021.
//

#ifndef INC_5300_BUTTERFLY_SQL5300_H
#define INC_5300_BUTTERFLY_SQL5300_H


class sql5300 {
public:

    // Constructor
    sql5300();

    // Execute function:
    //  Takes in an AST and converts it to string and prints the response
    // SQLParserResult*
    //TODO: Fix execute() by adding query parameter (AST type?)
    std::string execute();

};


#endif //INC_5300_BUTTERFLY_SQL5300_H
