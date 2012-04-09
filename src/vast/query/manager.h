#ifndef VAST_QUERY_MANAGER_H
#define VAST_QUERY_MANAGER_H

#include <mutex>
#include <vector>
#include "vast/query/query.h"

namespace vast {
namespace query {

/// Manages queries
class manager 
{
    manager(manager const&) = delete;
    manager& operator=(manager) = delete;

public:
    manager() = default;

    void process(query&& q);

private:
    std::mutex query_mutex_;
    std::vector<query> queries_;
};

} // namespace query
} // namespace vast

#endif
