#ifndef VAST_QUERY_SEARCH_H
#define VAST_QUERY_SEARCH_H

#include <unordered_map>
#include <ze/sink.h>
#include "vast/comm/event_source.h"
#include "vast/query/query.h"
#include "vast/store/forward.h"

namespace vast {
namespace query {

/// The search component.
class search : public ze::component
{
public:
    search(ze::io& io, store::archive& archive);
    search(search const&) = delete;
    search& operator=(search) = delete;

    /// Initializes the search component and start listening for Broccoli
    /// connections at a given endpoint.
    /// @param host The address or hostname where to listen.
    /// @param port The TCP port number to bind to.
    void init(std::string const& host, unsigned port);

    void stop();

    /// Creates a query from a query event.
    void submit(ze::event_ptr query_event);

private:
    /// Makes sure a query event has the right structure.
    void validate(ze::event const& event);

    store::archive& archive_;
    std::mutex query_mutex_;
    std::unordered_map<ze::uuid, query> queries_;
    std::unordered_map<ze::uuid, ze::uuid> query_to_emitter_;

    comm::event_source source_;
    ze::core_sink<ze::event> manager_;
};

} // namespace query
} // namespace vast

#endif
