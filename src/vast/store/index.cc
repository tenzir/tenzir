#include <vast/store/index.h>

#include <vast/store/segment.h>
#include <vast/util/logger.h>

namespace vast {
namespace store {

index::index(cppa::actor_ptr archive, std::string const& directory)
  : archive_(archive)
{
  LOG(verbose, store) << "spawning index @" << id();

  using namespace cppa;
  init_state = (
      on(atom("give"), arg_match) >> [=](actor_ptr query)
      {
        // TODO: compute the relevant set of segments based on the query.
        LOG(debug, store) 
          << "index @" << id() 
          << " asks archive to create emitter for @" << query->id();
        send(archive_, atom("emitter"), atom("create"), query);
      },
      on_arg_match >> [=](segment const& s)
      {
        LOG(debug, store) << "indexing segment " << s.id();
        // TODO: implement.
      },
      on(atom("shutdown")) >> [=]()
      {
        LOG(debug, store) << "shutting down";
        quit();
      });
}

} // namespace store
} // namespace vast
