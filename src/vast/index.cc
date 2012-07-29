#include "vast/index.h"

#include "vast/logger.h"
#include "vast/segment.h"

namespace vast {

index::index(cppa::actor_ptr archive, std::string const& directory)
  : archive_(archive)
{
  LOG(verbose, index) << "spawning index @" << id();

  using namespace cppa;
  init_state = (
      on(atom("give"), arg_match) >> [=](actor_ptr query)
      {
        // TODO: compute the relevant set of segments based on the query.
        LOG(debug, index)
          << "index @" << id()
          << " asks archive to create emitter for @" << query->id();
        send(archive_, atom("emitter"), atom("create"), query);
      },
      on_arg_match >> [=](segment const& s)
      {
        DBG(index) << "index @" << id() << " processes segment " << s.id();
        // TODO: implement.
      },
      on(atom("shutdown")) >> [=]()
      {
        quit();
        LOG(verbose, index) << "index @" << id() << " terminated";
      });
}

} // namespace vast
