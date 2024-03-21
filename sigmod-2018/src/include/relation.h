#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>

using RelationId = unsigned;
using TupleId = uint64_t;
using Index = std::unordered_map<uint64_t, std::vector<TupleId>>;

class Relation {
private:
    /// Owns memory (false if it was mmaped)
    bool owns_memory_ = true;
    /// The number of tuples
    uint64_t size_;
    /// The join column containing the keys
    std::vector<uint64_t *> columns_;

    /// The Indexes for every column
    std::vector<Index> indexes_;


public:
    /// Constructor without mmap
    Relation(uint64_t size, std::vector<uint64_t *> &&columns)
            : owns_memory_(true), size_(size), columns_(columns) {}
    /// Constructor using mmap
    explicit Relation(const char *file_name);
    /// Delete copy constructor
    Relation(const Relation &other) = delete;
    /// Move constructor
    Relation(Relation &&other) = default;

    /// The destructor
    ~Relation();

    /// Stores a relation into a file (binary)
    void storeRelation(const std::string &file_name);
    /// Stores a relation into a file (csv)
    void storeRelationCSV(const std::string &file_name);
    /// Dump SQL: Create and load table (PostgreSQL)
    void dumpSQL(const std::string &file_name, unsigned relation_id);

    /// The number of tuples
    uint64_t size() const {
        return size_;
    }
    /// The join column containing the keys
    const std::vector<uint64_t *> &columns() const {
        return columns_;
    }

    /// Build an index for all column
    void buildIndex();
    /// Match with Index and Return matched tuple ids
    std::vector<TupleId> match(const uint64_t key, const uint64_t column_id) const;

private:
    /// Loads data from a file
    void loadRelation(const char *file_name);
};

