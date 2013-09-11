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
    VAST_LOG_ACT_DEBUG("partition",
                       "loads last modification time from " <<
                       last_modified_file << ": " << last_modified_);
  }

  become(
      on(atom("meta"), atom("timestamp")) >> [=]
      {
        reply(last_modified_);
      },
      on(atom("load")) >> [=]
      {
        meta_ = spawn<meta_fragment>(dir_ / "meta");
        meta_ = spawn<type_fragment>(dir_ / "type");
        meta_ << last_dequeued();
        type_ << last_dequeued();
      },
      on_arg_match >> [=](event const&)
      {
        last_modified_ = now();
        meta_ << last_dequeued();
        type_ << last_dequeued();
      },
      on(atom("kill")) >> [=]
      {
        VAST_LOG_ACT_DEBUG("partition",
                           "saves last modification time " << last_modified_ <<
                           " to " << last_modified_file);
        io::archive(last_modified_file, last_modified_);
        meta_ << last_dequeued();
        type_ << last_dequeued();
        quit();
      });
}

void partition::on_exit()
{
  VAST_LOG_ACT_VERBOSE("partition", "terminated");
}


} // namespace vast
