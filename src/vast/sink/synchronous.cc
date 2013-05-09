#include "vast/sink/synchronous.h"

#include <ze/event.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

void synchronous::init()
{
  LOG(verbose, emit) << "spawning event sink @" << id();

  chaining(false);
  become(
      on(atom("process"), arg_match) >> [=](ze::event const& e)
      {
        process(e);
        ++total_events_;
      },
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, emit) << "event sink @" << id() << " terminated";
      });
}

} // namespace vast
