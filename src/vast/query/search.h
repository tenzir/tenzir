#ifndef VAST_QUERY_SEARCH_H
#define VAST_QUERY_SEARCH_H

#include <ze/sink.h>
#include "vast/comm/event_source.h"
#include "vast/query/manager.h"

namespace vast {
namespace query {

/// The search component.
struct search : public ze::component
{
    search(ze::io& io);
    search(search const&) = delete;
    search& operator=(search) = delete;

    comm::event_source source;
    ze::core_sink<ze::event> sink;
    manager mgr;
};

} // namespace query
} // namespace vast

#endif
