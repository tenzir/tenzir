#include "vast/partition.h"

#include "vast/event.h"
#include "vast/fragment.h"
#include "vast/logger.h"
#include "vast/io/serialization.h"

using namespace cppa;

namespace vast {

partition::partition(path dir)
  : dir_{std::move(dir)},
    last_modified_{now()}
{
}

void partition::init()
{
  VAST_LOG_ACT_VERBOSE("partition", "spawned");

  if (! exists(dir_))
  {
    VAST_LOG_ACT_DEBUG("partition", "creates new directory " << dir_);
    mkdir(dir_);
  }

  auto last_modified_file = dir_ / "last_modified";
  if (exists(last_modified_file))
  {
    io::unarchive(last_modified_file, last_modified_);
    VAST_LOG_ACT_DEBUG("partition", "loads last modification time: " << 
                       last_modified_);
  }

  meta_ = spawn<meta_fragment>(dir_ / "meta");
  type_ = spawn<type_fragment>(dir_ / "type");

  become(
      on(atom("kill")) >> [=]
      {
        VAST_LOG_ACT_DEBUG(
            "partition", "saves last modification time " << last_modified_ << 
            " to " << last_modified_file.trim(-2));
        io::archive(last_modified_file, last_modified_);
        meta_ << last_dequeued();
        type_ << last_dequeued();
        for (auto& p : events_)
          p.second << last_dequeued();
        quit();
      },
      on(atom("meta"), atom("timestamp")) >> [=]
      {
        reply(last_modified_);
      },
      on_arg_match >> [=](event const& e)
      {
        last_modified_ = now();
        meta_ << last_dequeued();
        type_ << last_dequeued();

        auto& a = events_[e.name()];
        if (! a)
          a = spawn<argument_fragment>(dir_ / "event"/ e.name());
        a << last_dequeued();
      });
}

void partition::on_exit()
{
  VAST_LOG_ACT_VERBOSE("partition", "terminated");
}

} // namespace vast
