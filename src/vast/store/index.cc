#include <vast/store/index.h>

#include <vast/store/segment.h>
#include <vast/util/logger.h>

namespace vast {
namespace store {

index::index(cppa::actor_ptr archive, std::string const& directory)
  : archive_(archive)
{
  using namespace cppa;
  init_state = (
      on_arg_match >> [=](segment const& s)
      {
        LOG(debug, store) << "[index] indexing segment " << s.id();
        // TODO.
      },
      on(atom("shutdown")) >> [=]()
      {
        LOG(debug, store) << "[index] shutting down";
        self->quit();
      });
}

} // namespace store
} // namespace vast
