#ifndef VAST_QUERY_SEARCH_H
#define VAST_QUERY_SEARCH_H

#include <unordered_map>
#include <ze/event.h>
#include <ze/vertex.h>
#include "vast/query/query.h"
#include "vast/store/forward.h"

namespace vast {
namespace query {

/// The search component.
///
/// The communication protocol uses the following events:
///
/// - `vast::query(action: string, options: table[string] of string)`
///
///     Allowed values for `action` are:
///
///         - `create`. Creates a query. The `options` table must contain a
///           `destination` value that represents a valid endpoint.
///
///         - `remove`
///
///         - `control`
///
///         - `statistics`
///
///     Allowed values for `options` are:
///
///         - `id`. The UUID of a query assigned by VAST.
///
///         - `aspect`. The control aspect when `action` has the value
///           `control`.
///
///         - `destination`. The endpoint of a query result in the form
///           `address:port`.
///
///         - `expression`. A query expression.
///
///         - `batch size`. The number of query results to process at once
///            before asking the user to proceed with the execution.
///
/// - `vast::nack(msg: string, ....)`
/// - `vast::ack(msg: string, ...)`
///
class search : public ze::component
{
public:
    search(ze::io& io, store::archive& archive);
    search(search const&) = delete;
    search& operator=(search) = delete;

    /// Initializes the search component.
    /// @param host The address or hostname where to listen.
    /// @param port The TCP port number to bind to.
    void init(std::string const& host, unsigned port);

    /// Stops the search component.
    void stop();

private:
    template <typename ...Args>
    void reply(char const* msg, std::vector<ze::zmq::message>& route,
               Args&& ...args)
    {
        ze::event event(msg, std::forward<Args>(args)...);
        manager_.send_with_route(event, std::move(route));
    }

    template <typename ...Args>
    void ack(std::vector<ze::zmq::message>& route, Args&& ...args)
    {
        reply("vast::ack", route, std::forward<Args>(args)...);
    }

    template <typename ...Args>
    void nack(std::vector<ze::zmq::message>& route, Args&& ...args)
    {
        reply("vast::nack", route, std::forward<Args>(args)...);
    }

    store::archive& archive_;
    std::mutex query_mutex_;
    std::unordered_map<ze::uuid, std::unique_ptr<query>> queries_;
    std::unordered_map<ze::uuid, ze::uuid> query_to_emitter_;

    ze::serial_router<> manager_;
};

} // namespace query
} // namespace vast

#endif
