#include "vast/query.h"

#include "vast/event.h"
#include "vast/exception.h"
#include "vast/logger.h"

using namespace cppa;

namespace vast {

query::query(cppa::actor_ptr archive,
             cppa::actor_ptr index,
             cppa::actor_ptr sink,
             expr::ast ast)
  : archive_{std::move(archive)},
    index_{std::move(index)},
    sink_{std::move(sink)},
    ast_{std::move(ast)}
{
}

void query::act()
{
  become(
      on(atom("run")) >> [=]
      {
        VAST_LOG_ACTOR_DEBUG("hits index");
        send(index_, atom("lookup"), ast_);
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR("got unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(last_dequeued()));
      });
}

char const* query::description() const
{
  return "query";
}

} // namespace vast
