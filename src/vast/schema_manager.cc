#include "vast/schema_manager.h"

#include <cppa/cppa.hpp>
#include "vast/schema.h"

using namespace cppa;

namespace vast {

void schema_manager::act()
{
  become(
      on(atom("load"), arg_match) >> [=](std::string const& file)
      {
        schema_.read(file);
      },
      on(atom("schema")) >> [=]()
      {
        return make_any_tuple(schema_);
      });
}

char const* schema_manager::description() const
{
  return "schema-manager";
}

} // namespace vast
