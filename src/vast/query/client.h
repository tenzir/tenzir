#ifndef VAST_QUERY_CLIENT_H
#define VAST_QUERY_CLIENT_H

#include <mutex>
#include <ze/event.h>
#include <ze/util/queue.h>
#include <ze/vertex.h>
#include "vast/query/query.h"
#include "vast/store/forward.h"

namespace vast {
namespace query {

/// A simple query client.
class client : public ze::component
{
    client(client const&) = delete;
    client& operator=(client) = delete;

public:
    client(ze::io& io);

    /// Initializes the query client.
    /// @param host The IP address of VAST's search component.
    /// @param port The TCP port of VAST's search component.
    /// @param expression The query expression
    /// @param batch_size Number of results per page.
    void init(std::string const& host,
              unsigned port,
              std::string const& expression,
              unsigned batch_size = 0u);

    /// Stops the query client.
    void stop();

    /// Wait for console input on STDIN.
    void wait_for_input();

private:
    bool try_print();

    ze::util::queue<ze::event> buffer_;
    ze::serial_dealer<> control_;
    ze::serial_dealer<> data_;
    std::string query_;
    unsigned batch_size_;
    unsigned printed_;
    bool asking_;
    std::mutex print_mutex_;
};

} // namespace query
} // namespace vast

#endif
