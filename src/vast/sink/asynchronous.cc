#include "vast/sink/asynchronous.h"

#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/exception.h"

using namespace cppa;

namespace vast {
namespace sink {

void asynchronous::act()
{
  become(
      on_arg_match >> [=](event const& e)
      {
        VAST_LOG_ACTOR_DEBUG(description(), "got 1 event");
        process(e);
        ++total_events_;
      },
      on_arg_match >> [=](std::vector<event> const& v)
      {
        VAST_LOG_ACTOR_DEBUG(description(), "got " << v.size() << " events");
        process(v);
        total_events_ += v.size();
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR(
            description(), "received unexpected message from @" <<
            last_sender()->id() << ": " << to_string(last_dequeued()));
      });
}

void asynchronous::on_exit()
{
  if (total_events_ > 0)
    VAST_LOG_ACTOR_VERBOSE(
        description(), "processed " << total_events_ << " events in total");
  actor<asynchronous>::on_exit();
}

size_t asynchronous::total_events() const
{
  return total_events_;
}

void asynchronous::process(std::vector<event> const& v)
{
  for (auto& e : v)
    process(e);
}

} // namespace sink
} // namespace vast
