#include <iostream>
#include <csignal>
#include <fcntl.h>
#include <fstream>

#include "joiner.h"
#include "parser.h"

int main(int argc, char *argv[]) {
    // argv[1] is the number of threads
    Joiner joiner;
    if (argc > 1) {
        joiner.setNumThreads(std::stoi(argv[1]));
    } else {
        joiner.setNumThreads(5);
    }
    // Read join relations
    std::string line;
    while (getline(std::cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    QueryInfo i;
    size_t query_id = 0;
    size_t batch_size = 0;
    std::map<size_t, std::string> responses;
    while (getline(std::cin, line)) {
        if (line == "F") {
            joiner.setBatchSize(batch_size);
            batch_size = 0;
            joiner.printCheckSum();
            continue;
        }
        i.parseQuery(line, query_id++);
        joiner.scheduleQuery(i);
        batch_size++;
    }
    return 0;
}
