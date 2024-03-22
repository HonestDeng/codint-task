#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <set>

#include "relation.h"
#include "parser.h"

namespace std {
/// Simple hash function to enable use with unordered_map
    template<>
    struct hash<SelectInfo> {
        std::size_t operator()(SelectInfo const &s) const noexcept {
            return s.binding ^ (s.col_id << 5);
        }
    };
}

/// Operators materialize their entire result
class Operator {
protected:
    /// Mapping from select info to data
//    std::unordered_map<SelectInfo, unsigned> select_to_result_col_id_;
    /// The late-materialized results
    std::vector<TupleId *> result_columns_;
    /// The tmp results. tmp_results[i] is the list of tid filtered from the i-th table
    std::vector<std::vector<TupleId>> tmp_results_;
    /// The result size
    uint64_t result_size_ = 0;
    /// The query context
    std::shared_ptr<Context> context_;

public:
    /// The destructor
    virtual ~Operator() = default;;

    /// Require a column and add it to results
    virtual bool require(SelectInfo info) = 0;

    /// Resolves a column
    unsigned resolve(SelectInfo info) {
        return 0;
    }

    /// Run
    virtual void run() = 0;

    /// Get  late-materialized results
    virtual std::vector<std::vector<TupleId>> *getResults();

    uint64_t result_size() const {
        return result_size_;
    }
};

class Scan : public Operator {
protected:
    /// The relation
    const Relation &relation_;
    /// The name of the relation in the query
    unsigned relation_binding_;

public:
    /// The constructor
    Scan(const Relation &r, unsigned relation_binding, std::shared_ptr<Context> context)
            : relation_(r), relation_binding_(relation_binding) {
        context_ = std::move(context);
        for (int i = 0; i < context_->relations_.size(); i++) {
            tmp_results_.emplace_back();
        }
    };

    /// Require a column and add it to results
    bool require(SelectInfo info) override;

    /// Run
    void run() override;

    /// Get  late-materialized results
    virtual std::vector<std::vector<TupleId>> *getResults() override;
};

class FilterScan : public Scan {
private:
    /// The filter info
    std::vector<FilterInfo> filters_;
    /// The input data
    std::vector<uint64_t *> input_data_;

private:
    /// Apply filter
    bool applyFilter(uint64_t id, FilterInfo &f);

    /// Copy tuple to result
    void copy2Result(uint64_t id);

public:
    /// The constructor
    FilterScan(const Relation &r, std::vector<FilterInfo> filters, std::shared_ptr<Context> context)
            : Scan(r,
                   filters[0].filter_column.binding, std::move(context)),
              filters_(filters) {};

    /// The constructor
    FilterScan(const Relation &r, FilterInfo &filter_info, std::shared_ptr<Context> context)
            : FilterScan(r,
                         std::vector<
                                 FilterInfo>{
                                 filter_info
                         }, std::move(context)) {};

    /// Require a column and add it to results
    bool require(SelectInfo info) override;

    /// Run
    void run() override;

    /// Get  late-materialized results
    virtual std::vector<std::vector<TupleId>> *getResults() override {
        return Operator::getResults();
    }
};

class Join : public Operator {
private:
    /// The input operators
    std::unique_ptr<Operator> left_, right_;
    /// The join predicate info
    PredicateInfo p_info_;

    using HT = std::unordered_multimap<uint64_t, uint64_t>;

    /// The hash table for the join
    HT hash_table_;
    /// Columns that have to be materialized
    std::unordered_set<SelectInfo> requested_columns_;
    /// Left/right columns that have been requested
    std::vector<SelectInfo> requested_columns_left_, requested_columns_right_;
    /// The input data that has to be copied
    std::vector<std::vector<TupleId>> *left_input_, *right_input_;

private:
    /// Copy tuple to result
    void copy2Result(uint64_t left_id, uint64_t right_id);

public:
    /// The constructor
    Join(std::unique_ptr<Operator> &&left,
         std::unique_ptr<Operator> &&right,
         const PredicateInfo &p_info, std::shared_ptr<Context> context)
            : left_(std::move(left)), right_(std::move(right)), p_info_(p_info) {
        context_ = std::move(context);
        for (int i = 0; i < context_->relations_.size(); i++) {
            tmp_results_.emplace_back();
        }
    };

    /// Require a column and add it to results
    bool require(SelectInfo info) override;

    /// Run
    void run() override;
};

class SelfJoin : public Operator {
private:
    /// The input operators
    std::unique_ptr<Operator> input_;
    /// The join predicate info
    PredicateInfo p_info_;
    /// The required IUs
    std::set<SelectInfo> required_IUs_;
    /// The entire input data
    std::vector<std::vector<TupleId>>* input_data_;

private:
    /// Copy tuple to result
    void copy2Result(uint64_t id);

public:
    /// The constructor
    SelfJoin(std::unique_ptr<Operator> &&input, PredicateInfo &p_info, std::shared_ptr<Context> context)
            : input_(std::move(input)), p_info_(p_info) {
        context_ = std::move(context);
        for (int i = 0; i < context_->relations_.size(); i++) {
            tmp_results_.emplace_back();
        }
    };

    /// Require a column and add it to results
    bool require(SelectInfo info) override;

    /// Run
    void run() override;
};

class Checksum : public Operator {
private:
    /// The input operator
    std::unique_ptr<Operator> input_;
    /// The join predicate info
    const std::vector<SelectInfo> col_info_;

    std::vector<uint64_t> check_sums_;

public:
    /// The constructor
    Checksum(std::unique_ptr<Operator> &&input,
             std::vector<SelectInfo> col_info, std::shared_ptr<Context> context)
            : input_(std::move(input)), col_info_(std::move(col_info)) {
        context_ = std::move(context);
    };

    /// Request a column and add it to results
    bool require(SelectInfo info) override {
        // check sum is always on the highest level
        // and thus should never request anything
        throw;
    }

    /// Run
    void run() override;

    const std::vector<uint64_t> &check_sums() {
        return check_sums_;
    }
};

