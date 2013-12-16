#include "vast/index.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/segment.h"
#include "vast/expression.h"
#include "vast/partition.h"
#include "vast/io/serialization.h"

using namespace cppa;

namespace vast {

index_actor::index_actor(path directory)
  : dir_{std::move(directory)}
{
}

char const* index_actor::description() const
{
  return "index";
}

void index_actor::act()
{
  chaining(false);
  become(
      on_arg_match >> [=](expr::ast const& ast)
      {
        check_partition();
        send(active_, ast, last_sender());
        VAST_LOG_ACTOR_DEBUG("sends predicate " << ast <<
                             " to partition " << VAST_ACTOR_ID(active_));
      },
      on(arg_match) >> [=](segment const& s)
      {
        check_partition();
        forward_to(active_);
      });

  auto latest = time_point{0};
  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        auto part = spawn<partition_actor, linked>(p);
        auto id = uuid{to_string(p.basename())};
        partitions_.emplace(id, part);

        time_point tp;
        assert(exists(p / "last_modified"));
        io::unarchive(p / "last_modified", tp);
        if (tp > latest)
        {
          latest = tp;
          active_ = part;
        }

        return true;
      });

  if (active_)
    VAST_LOG_ACTOR_DEBUG(
        "marked partition " << VAST_ACTOR_ID(active_) <<
        " as active (" << latest << ")");
}

void index_actor::check_partition()
{
  if (! partitions_.empty())
    return;

  assert(! exists(dir_));
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

} // namespace vast
