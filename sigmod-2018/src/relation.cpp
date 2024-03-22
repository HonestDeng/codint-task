#include "relation.h"

#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <csignal>

// Stores a relation into a binary file
void Relation::storeRelation(const std::string &file_name) {
    std::ofstream out_file;
    out_file.open(file_name, std::ios::out | std::ios::binary);
    out_file.write((char *) &size_, sizeof(size_));
    auto numColumns = columns_.size();
    out_file.write((char *) &numColumns, sizeof(size_t));
    for (auto c : columns_) {
        out_file.write((char *) c, size_ * sizeof(uint64_t));
    }
    out_file.close();
}

// Stores a relation into a file (csv), e.g., for loading/testing it with a DBMS
void Relation::storeRelationCSV(const std::string &file_name) {
    std::ofstream out_file;
    out_file.open(file_name, std::ios::out);
    for (uint64_t i = 0; i < size_; ++i) {
        for (auto &c : columns_) {
            out_file << c[i] << ',';
        }
        out_file << "\n";
    }
}

// Dump SQL: Create and load table (PostgreSQL)
void Relation::dumpSQL(const std::string &file_name, unsigned relation_id) {
    std::ofstream out_file;
    out_file.open(file_name + ".sql", std::ios::out);
    // Create table statement
    out_file << "CREATE TABLE r" << relation_id << " (";
    for (unsigned cId = 0; cId < columns_.size(); ++cId) {
        out_file << "c" << cId << " bigint"
                 << (cId < columns_.size() - 1 ? "," : "");
    }
    out_file << ");\n";
    // Load from csv statement
    out_file << "copy r" << relation_id << " from 'r" << relation_id
             << ".tbl' delimiter '|';\n";
}

void Relation::loadRelation(const char *file_name) {
    owns_memory_ = true;

    int fd = open(file_name, O_RDONLY);
    // 获取文件大小
    struct stat sb{};
    if (fstat(fd, &sb) == -1)
        std::cerr << "fstat\n";

    auto length = sb.st_size;  // 文件大小
    close(fd);

    // TODO： 根据文件大小，判断是否需要mmap

    // 重新打开文件正式读取数据
    std::ifstream is = std::ifstream(file_name, std::ios::in | std::ios::binary);
    if (!is) {
        std::cerr << "cannot open " << file_name << std::endl;
        throw;
    }

    // 首先读取size_和numColumns
    is.read((char *) &size_, sizeof(size_));
    // 然后读取关系表属性的数量
    size_t numColumns;
    is.read((char *) &numColumns, sizeof(size_t));
    // 读取每一列的数据
    for (unsigned i = 0; i < numColumns; ++i) {
        auto *column = new uint64_t[size_];
        is.read((char *) column, size_ * sizeof(uint64_t));
        columns_.push_back(column);
    }
}

// Constructor that loads relation_ from disk
Relation::Relation(const char *file_name) : owns_memory_(true), size_(0) {
    loadRelation(file_name);
}

// Destructor
Relation::~Relation() {
    if (owns_memory_) {
        for (auto c : columns_)
            delete[] c;
    }
}

// Build an index for all column
void Relation::buildIndex() {
    for(auto & column : columns_) {
        Index index;
        for (uint64_t i = 0; i < size_; ++i) {
            index[column[i]].push_back(i);
        }
        indexes_.push_back(index);
    }
}

// Build Match with Index
std::vector<TupleId> Relation::match(const uint64_t key, const uint64_t column_id) const {
    auto it = indexes_[column_id].find(key);
    if (it == indexes_[column_id].end()) {
        return std::vector<uint64_t>();
    }
    return it->second;
}
