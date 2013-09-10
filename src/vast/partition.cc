#include "vast/partition.h"

#include "vast/event.h"
#include "vast/fragment.h"
#include "vast/logger.h"

using namespace cppa;

namespace vast {

partition::partition(path dir)
  : dir_{std::move(dir)},
    last_modified_{now()}
{
}

void partition::init()
{
  VAST_LOG_VERBOSE(VAST_ACTOR("partition") << " spawned");

  become(
      on(atom("get"), atom("timestamp")) >> [=]
      {
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
        meta_ << last_dequeued();
        type_ << last_dequeued();
        quit();
      });
}

void partition::on_exit()
{
  VAST_LOG_VERBOSE(VAST_ACTOR("partition") << " terminated");
}


} // namespace vast
