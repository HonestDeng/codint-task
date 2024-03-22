#include "operators.h"

// Get late-materialized results
std::vector<std::vector<TupleId>>* Operator::getResults() {
    return &tmp_results_;
}

// Require a column and add it to results
bool Scan::require(SelectInfo info) {
    return true;
}

// Run
void Scan::run() {
    // Nothing to do
    result_size_ = relation_.size();
}

// Get late-materialized results
std::vector<std::vector<TupleId>>* Scan::getResults() {
    for (uint64_t i = 0; i < relation_.size(); i++) {
        tmp_results_[relation_binding_].push_back(i);
    }
    return Operator::getResults();
}

// Require a column and add it to results
bool FilterScan::require(SelectInfo info) {
    // require函数的作用是告诉当前的这个算子，info指定的这个列在join的过程中需要用到，需要把这个列加入到结果中
    return true;
}

// Copy to result
void FilterScan::copy2Result(TupleId id) {
    tmp_results_[filters_[0].filter_column.binding].push_back(id);
    ++result_size_;
}

// Apply filter
bool FilterScan::applyFilter(uint64_t i, FilterInfo &f) {
    // i是元组的id
    auto compare_col = relation_.columns()[f.filter_column.col_id];
    auto constant = f.constant;
    switch (f.comparison) {
        case FilterInfo::Comparison::Equal:
            return compare_col[i] == constant;
        case FilterInfo::Comparison::Greater:
            return compare_col[i] > constant;
        case FilterInfo::Comparison::Less:
            return compare_col[i] < constant;
    }
    return false;
}

// Run
void FilterScan::run() {
    for (uint64_t i = 0; i < relation_.size(); ++i) {
        bool pass = true;
        for (auto &f: filters_) {
            pass &= applyFilter(i, f);
        }
        if (pass)
            copy2Result(i);
    }
}

// Require a column and add it to results
bool Join::require(SelectInfo info) {
    return true;
}

// Copy to result
void Join::copy2Result(uint64_t left_id, uint64_t right_id) {
    // left_id和right_id是左右两个late-materialized结果的行号

    // 先把left input中的tuple id放到tmp_results_中
    unsigned max_binding = context_->relations_.size();
    for(unsigned binding = 0; binding < max_binding; binding++) {
        if((*left_input_)[binding].empty()) {
            continue;
        }
        const auto& input = (*left_input_)[binding];
        tmp_results_[binding].push_back(input[left_id]);
    }
    // 这里需要保证左表包含的binding与右表包含的binding不会重复
    for(unsigned binding = 0; binding < max_binding; binding++) {
        if((*right_input_)[binding].empty()) {
            continue;
        }
        const auto& input = (*right_input_)[binding];
        tmp_results_[binding].push_back(input[right_id]);
    }
    result_size_++;
}

// Run
void Join::run() {
    left_->run();
    right_->run();


    // Use smaller input_ for build
    if (left_->result_size() > right_->result_size()) {
        std::swap(left_, right_);
        std::swap(p_info_.left, p_info_.right);
        std::swap(requested_columns_left_, requested_columns_right_);
    }

    left_input_ = left_->getResults();
    right_input_ = right_->getResults();

    // Build phase
    auto left_key_column = context_->getColumn(p_info_.left);
    hash_table_.reserve(left_->result_size() * 2);
    for (uint64_t i = 0, limit = i + left_->result_size(); i != limit; ++i) {
        TupleId id = (*left_input_)[p_info_.left.binding][i];
        hash_table_.emplace(left_key_column[id], i);
    }
    // Probe phase
    auto right_key_column = context_->getColumn(p_info_.right);
    for (uint64_t i = 0, limit = i + right_->result_size(); i != limit; ++i) {
        auto tuple_id = (*right_input_)[p_info_.right.binding][i];
        auto rightKey = right_key_column[tuple_id];
        auto range = hash_table_.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
            copy2Result(iter->second, i);
        }
    }
}

// Copy to result
void SelfJoin::copy2Result(uint64_t id) {
    size_t max_binding = context_->relations_.size();
    for(int binding = 0; binding < max_binding; binding++) {
        if((*input_data_)[binding].empty()){
            continue;
        }
        tmp_results_[binding].push_back((*input_data_)[binding][id]);
    }
    ++result_size_;
}

// Require a column and add it to results
bool SelfJoin::require(SelectInfo info) {
    return true;
}

// Run
void SelfJoin::run() {
    input_->run();
    input_data_ = input_->getResults();

    auto left_col = context_->getColumn(p_info_.left);
    auto right_col = context_->getColumn(p_info_.right);
    for (uint64_t i = 0; i < input_->result_size(); ++i) {
        auto right_tuple_id = (*input_data_)[p_info_.right.binding][i];
        auto left_tuple_id = (*input_data_)[p_info_.left.binding][i];

        if (left_col[left_tuple_id] == right_col[right_tuple_id])
            copy2Result(i);
    }
}

// Run
void Checksum::run() {
    input_->run();
    auto results = input_->getResults();

    for (auto &sInfo: col_info_) {
        auto result_col = context_->getColumn(sInfo);
        uint64_t sum = 0;
        result_size_ = input_->result_size();
        for(const TupleId id: (*results)[sInfo.binding]) {
            sum += result_col[id];
        }
        check_sums_.push_back(sum);
    }
}

