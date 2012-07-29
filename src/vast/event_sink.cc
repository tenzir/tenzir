#include "vast/event_sink.h"

#include <ze/event.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

event_sink::event_sink()
{
  LOG(verbose, emit) << "spawning event sink @" << id();

  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("process"), arg_match) >> [=](ze::event const& event)
      {
        if (finished_)
        {
          reply(atom("sink"), atom("done"));
          return;
        }

        process(event);
        ++total_events_;
      },
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, emit) << "event sink @" << id() << " terminated";
      });
}

} // namespace vast
