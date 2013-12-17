#include "vast/index.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/segment.h"
#include "vast/expression.h"
#include "vast/partition.h"
#include "vast/uuid.h"
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
  trap_exit(true);

  auto latest = time_point{0};
  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        // If the meta data of a partition does not exist, we have a file
        // system inconsistency and ignore the partition entirely.
        if (! exists(p / "last_modified") || ! exists(p / "coverage"))
        {
          VAST_LOG_ACTOR_WARN("missing meta data for partition " << p);
          return true;
        }

        auto part = spawn<partition_actor, monitored>(p);
        partitions_.emplace(part);

        time_point tp;
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

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (partitions_.empty())
          quit(reason);
        else
          for (auto& p : partitions_)
            send_exit(p, reason);
      },

      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        VAST_LOG_ACTOR_DEBUG("got DOWN from " << VAST_ACTOR_ID(last_sender()));

        partitions_.erase(last_sender());

        if (reason == exit::stop)
        {
          if (partitions_.empty())
            quit(exit::stop);
        }
        else
        {
          if (reason != exit::error)
            VAST_LOG_ACTOR_WARN(
                "terminates with unknown exit code from " <<
               VAST_ACTOR_ID(last_sender()) << ": " << reason);

          quit(exit::error);
        }
      },

      on_arg_match >> [=](expr::ast const& ast)
      {
        if (partitions_.empty())
        {
          VAST_LOG_ACTOR_WARN("has no partitions to answer: " << ast);
          return;
        }

        VAST_LOG_ACTOR_DEBUG("sends predicate " << ast <<
                             " to partition " << VAST_ACTOR_ID(active_));

        send(active_, ast, last_sender());
      },

      on_arg_match >> [=](segment const& s)
      {
        if (partitions_.empty())
        {
          if (! exists(dir_) && ! mkdir(dir_))
          {
            VAST_LOG_ACTOR_ERROR("failed to create index directory: " << dir_);
            return make_any_tuple(atom("segment"), atom("nack"), s.id());
            quit(exit::error);
          }

          auto id = uuid::random();
          VAST_LOG_ACTOR_INFO("creates new partition " << id);
          auto p = spawn<partition_actor, monitored>(dir_ / to<string>(id));
          active_ = p;
          partitions_.emplace(std::move(p));
        }

        forward_to(active_);
        return make_any_tuple(atom("segment"), atom("ack"), s.id());
      },

      on(atom("delete")) >> [=]
      {
        if (partitions_.empty())
        {
          VAST_LOG_ACTOR_WARN("ignores request to delete empty index");
          return;
        }

        become(
            keep_behavior,
            on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
            {
              if (reason != exit::kill)
                VAST_LOG_ACTOR_WARN(
                    "got DOWN from " << VAST_ACTOR_ID(last_sender()) <<
                    " with unexpected exit code " << reason);

              partitions_.erase(last_sender());
              if (partitions_.empty())
              {
                if (! rm(dir_))
                {
                  VAST_LOG_ACTOR_ERROR("failed to delete index directory: " <<
                                       dir_);
                  quit(exit::error);
                  return;
                }

                VAST_LOG_ACTOR_INFO("deleted index: " << dir_);
                unbecome();
              }
            }
        );

        for (auto& p : partitions_)
          send_exit(p, exit::kill);
      });

}

} // namespace vast
