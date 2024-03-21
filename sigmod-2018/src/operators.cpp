#include "operators.h"

// Get late-materialized results
std::vector<std::vector<TupleId>>* Operator::getResults() {
    return &tmp_results_;
}

// Require a column and add it to results
bool Scan::require(SelectInfo info) {
//    if (info.binding != relation_binding_)
//        return false;
//    assert(info.col_id < relation_.columns().size());
//    result_columns_.push_back(relation_.columns()[info.col_id]);
//    select_to_result_col_id_[info] = result_columns_.size() - 1;
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
//    if (info.binding != relation_binding_)
//        return false;
//    assert(info.col_id < relation_.columns().size());
//    if (select_to_result_col_id_.find(info) == select_to_result_col_id_.end()) {
//        // Add to results
//        unsigned colId = tmp_results_.size() - 1;
//        select_to_result_col_id_[info] = colId;
//    }
    return true;
}

// Copy to result
void FilterScan::copy2Result(TupleId id) {
//    for (unsigned cId = 0; cId < input_data_.size(); ++cId)
//        tmp_results_[cId].push_back(input_data_[cId][id]);
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
//    if (requested_columns_.count(info) == 0) {
//        bool success = false;
//        if (left_->require(info)) {
//            requested_columns_left_.emplace_back(info);
//            success = true;
//        } else if (right_->require(info)) {
//            success = true;
//            requested_columns_right_.emplace_back(info);
//        }
//        if (!success)
//            return false;
//
//        tmp_results_.emplace_back();
//        requested_columns_.emplace(info);
//    }
    return true;
}

// Copy to result
void Join::copy2Result(uint64_t left_id, uint64_t right_id) {
    // left_id和right_id是左右两个late-materialized结果的行号
//    unsigned rel_col_id = 0;
//    for (unsigned cId = 0; cId < copy_left_data_.size(); ++cId)
//        tmp_results_[rel_col_id++].push_back(copy_left_data_[cId][left_id]);
//
//    for (unsigned cId = 0; cId < copy_right_data_.size(); ++cId)
//        tmp_results_[rel_col_id++].push_back(copy_right_data_[cId][right_id]);
//    ++result_size_;

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
//    left_->require(p_info_.left);
//    right_->require(p_info_.right);
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
//        if(right_tuple_id >= 330){
//            printf("i=%d, key=%d\n", right_tuple_id, rightKey);
//        }
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
//    if (required_IUs_.count(info))
//        return true;
//    if (input_->require(info)) {
//        tmp_results_.emplace_back();
//        required_IUs_.emplace(info);
//        return true;
//    }
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

