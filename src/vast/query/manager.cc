#include "vast/query/manager.h"

namespace vast {
namespace query {

void manager::process(query&& q)
{
    std::lock_guard<std::mutex> lock(query_mutex_);
    queries_.push_back(std::move(q));
}

} // namespace query
} // namespace vast
