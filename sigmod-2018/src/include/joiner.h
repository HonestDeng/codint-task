#pragma once

#include <vector>
#include <cstdint>
#include <set>
#include <optional>
#include <thread>
#include <future>
#include <queue>

#include "operators.h"
#include "relation.h"
#include "parser.h"

template <class T>
class Channel {
public:
    Channel() = default;
    ~Channel() = default;

    /**
     * @brief Inserts an element into a shared queue.
     *
     * @param element The element to be inserted.
     */
    void Put(T element) {
        std::unique_lock<std::mutex> lk(m_);
        q_.push(std::move(element));
        lk.unlock();
        cv_.notify_all();
    }

    /**
     * @brief Gets an element from the shared queue. If the queue is empty, blocks until an element is available.
     */
    auto Get() -> T {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&]() { return !q_.empty(); });
        T element = std::move(q_.front());
        q_.pop();
        return element;
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::queue<T> q_;
};


class Joiner {
private:
    /// The relations that might be joined
    std::vector<Relation> relations_;

    Channel<std::optional<QueryInfo>> request_queue_;
    using Response = std::pair<size_t, std::string>;
    Channel<std::optional<Response>> response_queue_;

    std::vector<std::thread> worker_threads_;

    unsigned num_t = 5;

public:
    /// Add relation
    void addRelation(const char *file_name);
    void addRelation(Relation &&relation);
    /// Get relation
    const Relation &getRelation(unsigned relation_id);
    /// Joins a given set of relations
    std::string join(QueryInfo &i);

    Joiner() {
        for(int i = 0; i < num_t; i++) {
            worker_threads_.emplace_back([&]{StartWorkerThread();});
        }
    }
    ~Joiner() {
        for(int i = 0; i < num_t; i++) {
            request_queue_.Put(std::nullopt);
        }
        for(auto &t: worker_threads_) {
            t.join();
        }
    }

    const std::vector<Relation> &relations() const {
        return relations_;
    }

    void StartWorkerThread();

    void scheduleQuery(std::optional<QueryInfo> query);

    void printCheckSum();

    size_t bs = 0;

private:
    /// Add scan to query
    std::unique_ptr<Operator> addScan(std::set<unsigned> &used_relations,
                                      const SelectInfo &info,
                                      QueryInfo &query, std::shared_ptr<Context> context);
};

