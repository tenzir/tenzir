#include "vast/index.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/segment.h"
#include "vast/expression.h"
#include "vast/partition.h"

using namespace cppa;

namespace vast {

index::index(path directory)
  : dir_{std::move(directory)}
{
}

char const* index::description() const
{
  return "index";
}

void index::act()
{
  chaining(false);
  become(
      on_arg_match >> [=](expr::ast const& ast)
      {
        // FIXME: Support more than 1 partition.
        auto part = partitions_.begin()->second;
        send(part, ast, last_sender());
        VAST_LOG_ACTOR_DEBUG("sends predicate " << ast <<
                             " to partition " << VAST_ACTOR_ID(part));
      },
      on(arg_match) >> [=](segment const& s)
      {
        assert(active_);
        reply(atom("segment"), atom("ack"), s.id());
        forward_to(active_);
      });

  if (! exists(dir_))
  {
    if (! mkdir(dir_))
    {
      VAST_LOG_ACTOR_ERROR("failed to create " << dir_);
      quit(exit::error);
      return;
    }
    auto id = uuid::random();
    VAST_LOG_ACTOR_INFO("creates new partition " << id);
    auto p = spawn<partition_actor, linked>(dir_ / to<string>(id));
    active_ = p;
    partitions_.emplace(std::move(id), std::move(p));
  }
  else
  {
    // FIXME: if the number of partitions get too large, there may be too many
    // sync_send handlers on the stack. We probably should directly read the
    // meta data from the filesystem.
    auto latest = std::make_shared<time_point>(0);
    traverse(
        dir_,
        [&](path const& p) -> bool
        {
          auto part = spawn<partition_actor, linked>(p);
          auto id = uuid{to_string(p.basename())};
          partitions_.emplace(id, part);
          sync_send(part, atom("timestamp")).then(
              on_arg_match >> [=](time_point tp)
              {
                if (tp >= *latest)
                {
                  VAST_LOG_ACTOR_DEBUG("marked partition " << p <<
                                       " as active (" << tp << ")");
                  *latest = tp;
                  active_ = part;
                }
              });
          return true;
        });
  }
}

} // namespace vast
