#include "vast/sink/asynchronous.h"

#include "vast/event.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {
namespace sink {

using namespace cppa;

void asynchronous::init()
{
  VAST_LOG_VERBOSE("spawning sink @" << id());
  self->trap_exit(true);
  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t /* reason */)
      {
        send(self, atom("kill"));
      },
      on_arg_match >> [=](event const& e)
      {
        VAST_LOG_DEBUG("sink @" << id() << " got 1 event");
        process(e);
        ++total_events_;
      },
      on_arg_match >> [=](std::vector<event> const& v)
      {
        VAST_LOG_DEBUG("sink @" << id() << " got " << v.size() << " events");
        process(v);
        total_events_ += v.size();
      },
      on(atom("kill")) >> [=]
      {
        before_exit();
        quit();
      },
      others() >> [=]
      {
        VAST_LOG_ERROR("sink @" << id() <<
                       " received unexpected message from @" <<
                       last_sender()->id() << ": " <<
                       to_string(last_dequeued()));
      });
}

void asynchronous::on_exit()
{
  if (total_events_ > 0)
    VAST_LOG_VERBOSE("sink @" << id() << " processed " <<
                     total_events_ << " events in total");

  VAST_LOG_VERBOSE("sink @" << id() << " terminated");
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

void asynchronous::before_exit()
{
}

} // namespace sink
} // namespace vast
