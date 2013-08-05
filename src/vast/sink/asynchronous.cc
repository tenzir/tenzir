#include "vast/sink/asynchronous.h"

#include "vast/event.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {
namespace sink {

using namespace cppa;

void asynchronous::init()
{
  VAST_LOG_VERBOSE("spawning event sink @" << id());

  chaining(false);
  become(
      on(atom("process"), arg_match) >> [=](event const& e)
      {
        process(e);
        ++total_events_;
      },
      on(atom("kill")) >> [=]
      {
        quit();
        VAST_LOG_VERBOSE("event sink @" << id() << " terminated");
      });
}

bool asynchronous::process(std::vector<event> const& v)
{
  auto success = true;
  for (auto& e : v)
    if (! process(e))
      success = false;
  return success;
}

} // namespace sink
} // namespace vast
