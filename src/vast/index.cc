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

index_actor::index_actor(path directory, size_t batch_size)
  : dir_{std::move(directory)},
    batch_size_{batch_size}
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

  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        if (! exists(p / "coverage"))
        {
          // If the meta data of a partition does not exist, we have a file
          // system inconsistency and ignore the partition.
          VAST_LOG_ACTOR_WARN("couldn't find meta data for partition " << p);
        }
        else
        {
          auto part = spawn<partition_actor, monitored>(p, batch_size_);
          partitions_.emplace(p.basename(), part);
        }

        return true;
      });

  if (! partitions_.empty())
  {
    auto active = partitions_.rbegin();
    VAST_LOG_ACTOR_INFO("sets existing partition as active: " << active->first);
    active_ = active->second;
  }

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (partitions_.empty())
          quit(reason);
        else
          for (auto& p : partitions_)
            send_exit(p.second, reason);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        VAST_LOG_ACTOR_DEBUG("got DOWN from " << VAST_ACTOR_ID(last_sender()));

        for (auto i = partitions_.begin(); i != partitions_.end(); ++i)
          if (i->second == last_sender())
          {
            partitions_.erase(i);
            break;
          }

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
      on(atom("partition"), arg_match) >> [=](std::string const& part)
      {
        auto part_dir = path{part};

        if (exists(dir_ / part_dir))
          VAST_LOG_ACTOR_INFO("appends to existing partition " << part);
        else
          VAST_LOG_ACTOR_INFO("creates new partition " << part);

        if (! partitions_.count(part_dir))
        {
          auto d = dir_ / part_dir;
          auto a = spawn<partition_actor, monitored>(d, batch_size_);
          partitions_.emplace(part_dir, a);
        }

        VAST_LOG_ACTOR_INFO("sets active partition to " << part_dir);
        active_ = partitions_[part_dir];
        assert(active_);
      },
      on_arg_match >> [=](expr::ast const& ast)
      {
        if (partitions_.empty())
        {
          VAST_LOG_ACTOR_WARN("has no partitions to answer: " << ast);
          return;
        }

        for (auto i = partitions_.rbegin(); i != partitions_.rend(); ++i)
        {
          send(i->second, ast, last_sender());
          VAST_LOG_ACTOR_DEBUG("sent predicate " << ast <<
                               " to partition " << VAST_ACTOR_ID(i->second));
        }
      },
      on_arg_match >> [=](segment const& s)
      {
        if (partitions_.empty())
        {
          auto id = uuid::random();
          VAST_LOG_ACTOR_INFO("creates new random partition " << id);
          auto part_dir = dir_ / to<string>(id);
          assert(! exists(part_dir));
          auto p = spawn<partition_actor, monitored>(part_dir, batch_size_);
          active_ = p;
          partitions_.emplace(part_dir.basename(), std::move(p));
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

              for (auto i = partitions_.begin(); i != partitions_.end(); ++i)
                if (i->second == last_sender())
                {
                  partitions_.erase(i);
                  break;
                }

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
          send_exit(p.second, exit::kill);
      });
}

} // namespace vast
