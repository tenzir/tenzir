#include "vast/query/search.h"

#include <ze/event.h>
#include <ze/link.h>
#include "vast/util/logger.h"
#include "vast/query/exception.h"

namespace vast {
namespace query {

search::search(ze::io& io)
  : ze::component(io)
  , source(*this)
  , sink(*this)
{
    ze::link(source, sink);
    sink.receive(
        [&](ze::event_ptr&& event)
        {
            if (event->name() != "__vast_query")
                throw exception("invalid query event name");

            if (event->size() != 1)
                throw exception("invalid number of query event arguments");

            if (! event->front().which() == ze::string_type)
                throw exception("invalid first argument type of query event");

            auto& str = event->front().get<ze::string>();
            query q({str.begin(), str.end()});
            mgr.process(std::move(q));
        });
}

} // namespace query
} // namespace vast
