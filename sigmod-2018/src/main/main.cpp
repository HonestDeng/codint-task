#include <iostream>
#include <csignal>
#include <fcntl.h>
#include <fstream>

#include "joiner.h"
#include "parser.h"

int main(int argc, char *argv[]) {
    Joiner joiner;
    // Read join relations
    std::string line;
    while (getline(std::cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    QueryInfo i;
    size_t query_id = 0;
    size_t bs = 0;
    std::map<size_t, std::string> responses;
    while (getline(std::cin, line)) {
        if (line == "F"){
            joiner.bs = bs;
            bs = 0;
            joiner.printCheckSum();
            continue;
        }
        i.parseQuery(line, query_id++);
        joiner.scheduleQuery(i);
        bs++;
    }
    return 0;
}
