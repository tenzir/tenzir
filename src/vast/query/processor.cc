#include "vast/query/processor.h"

#include <ze/event.h>
#include "vast/query/query.h"
#include "vast/util/logger.h"

namespace vast {
namespace query {

processor::processor(ze::component<ze::event>& c)
  : ze::core_sink<ze::event>(c)
{
}

void processor::init()
{
    receive([&](ze::event_ptr&& event) { process(std::move(event)); });
}

void processor::submit(query const& query)
{
}

void processor::process(ze::event_ptr&& event)
{
    LOG(debug, query) << "processing event" << *event;
}

} // namespace query
} // namespace vast
