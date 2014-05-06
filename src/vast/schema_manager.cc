#include "vast/schema_manager.h"

#include <cppa/cppa.hpp>
#include "vast/file_system.h"
#include "vast/schema.h"

using namespace cppa;

namespace vast {

void schema_manager_actor::act()
{
  become(
      on(atom("load"), arg_match) >> [=](std::string const& file)
      {
        auto contents = load(path{file});
        if (! contents)
        {
          VAST_LOG_ERROR("could not load schema: " << contents.error());
          return;
        }

        auto lval = contents->begin();
        auto sch = parse<schema>(lval, contents->end());
        if (! sch)
          VAST_LOG_ACTOR_ERROR(sch.error());

        schema_ = *sch;
      },
      on(atom("schema")) >> [=]()
      {
        return make_any_tuple(schema_);
      });
}

char const* schema_manager_actor::description() const
{
  return "schema-manager";
}

} // namespace vast
