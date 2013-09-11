#include "vast/schema_manager.h"

#include "vast/schema.h"
#include "vast/logger.h"

using namespace cppa;

namespace vast {

void schema_manager::init()
{
  VAST_LOG_ACT_VERBOSE("schema-manager", "spawned");
  become(
      on(atom("kill")) >> [=]
      {
        quit();
      },
      on(atom("load"), arg_match) >> [=](std::string const& file)
      {
        schema_.read(file);
      },
      on(atom("schema")) >> [=]()
      {
        reply(schema_);
      });
}

void schema_manager::on_exit()
{
  VAST_LOG_ACT_VERBOSE("schema-manager", "terminated");
}

} // namespace vast
