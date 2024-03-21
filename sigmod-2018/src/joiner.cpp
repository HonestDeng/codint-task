#include "joiner.h"

#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>

#include "parser.h"

namespace {

// Left表示左边的关系表已经用过了，Right表示右边的关系表已经用过了
    enum QueryGraphProvides {
        Left, Right, Both, None
    };

// Analyzes inputs of join
    QueryGraphProvides analyzeInputOfJoin(std::set<unsigned> &usedRelations,
                                          SelectInfo &leftInfo,
                                          SelectInfo &rightInfo) {
        bool used_left = usedRelations.count(leftInfo.binding);
        bool used_right = usedRelations.count(rightInfo.binding);

        if (used_left ^ used_right)
            return used_left ? QueryGraphProvides::Left : QueryGraphProvides::Right;
        if (used_left && used_right)
            return QueryGraphProvides::Both;
        return QueryGraphProvides::None;
    }

}

// Loads a relation_ from disk
void Joiner::addRelation(const char *file_name) {
    relations_.emplace_back(file_name);
    relations_.back().storeRelationCSV("r" + std::to_string(relations_.size() - 1) + ".csv");
}

void Joiner::addRelation(Relation &&relation) {
    relations_.emplace_back(std::move(relation));
}

// Loads a relation from disk
const Relation &Joiner::getRelation(unsigned relation_id) {
    if (relation_id >= relations_.size()) {
        std::cerr << "Relation with id: " << relation_id << " does not exist"
                  << std::endl;
        throw;
    }
    return relations_[relation_id];
}

// Add scan to query
std::unique_ptr<Operator> Joiner::addScan(std::set<unsigned> &used_relations,
                                          const SelectInfo &info,
                                          QueryInfo &query, std::shared_ptr<Context> context) {
    used_relations.emplace(info.binding);
    std::vector<FilterInfo> filters;
    for (auto &f: query.filters()) {
        if (f.filter_column.binding == info.binding) { // filter所作用的关系表和info的关系表一致
            // 则将filter添加到filters中
            filters.emplace_back(f);
        }
    }
    return !filters.empty() ?
           std::make_unique<FilterScan>(getRelation(info.rel_id), filters, context)
                            : std::make_unique<Scan>(getRelation(info.rel_id),
                                                     info.binding, context);
}

// Executes a join query
std::string Joiner::join(QueryInfo &query) {
    // 创建一个vector，用于存储需要用到的关系表
    std::vector<const Relation *> relations;
    for (const auto &rel_id: query.relation_ids()) {
        relations.push_back(&getRelation(rel_id));
    }
    auto q = std::make_shared<QueryInfo>(query);
    auto context = std::make_shared<Context>(relations, q);

    // 创建一个set，用于存储已经用过的关系表
    std::set<unsigned> used_relations;

    // We always start with the first join predicate and append the other joins
    // to it (--> left-deep join trees). You might want to choose a smarter
    // join ordering ...
    const auto &firstJoin = query.predicates()[0];
    std::unique_ptr<Operator> left, right;
    left = addScan(used_relations, firstJoin.left, query, context);
    right = addScan(used_relations, firstJoin.right, query, context);
    std::unique_ptr<Operator>
            root = std::make_unique<Join>(move(left), move(right), firstJoin, context);

    auto predicates_copy = query.predicates();
    for (unsigned i = 1; i < predicates_copy.size(); ++i) {
        auto &p_info = predicates_copy[i];
        auto &left_info = p_info.left;
        auto &right_info = p_info.right;

        switch (analyzeInputOfJoin(used_relations, left_info, right_info)) {
            case QueryGraphProvides::Left:
                left = move(root);
                right = addScan(used_relations, right_info, query, context);
                root = std::make_unique<Join>(move(left), move(right), p_info, context);
                break;
            case QueryGraphProvides::Right:
                left = addScan(used_relations,
                               left_info,
                               query, context);
                right = move(root);
                root = std::make_unique<Join>(move(left), move(right), p_info, context);
                break;
            case QueryGraphProvides::Both:
                // All relations of this join are already used somewhere else in the
                // query. Thus, we have either a cycle in our join graph or more than
                // one join predicate per join.
                root = std::make_unique<SelfJoin>(move(root), p_info, context);
                break;
            case QueryGraphProvides::None:
                // Process this predicate later when we can connect it to the other
                // joins. We never have cross products.
                predicates_copy.push_back(p_info);
                break;
        };
    }

    Checksum checksum(move(root), query.selections(), context);
    checksum.run();

    std::stringstream out;
    auto &results = checksum.check_sums();
    for (unsigned i = 0; i < results.size(); ++i) {
        out << (checksum.result_size() == 0 ? "NULL" : std::to_string(results[i]));
        if (i < results.size() - 1)
            out << " ";
    }
    out << "\n";
    return out.str();
}

