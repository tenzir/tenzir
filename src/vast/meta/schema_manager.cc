#include "vast/meta/schema_manager.h"

#include "vast/meta/taxonomy.h"
#include "vast/meta/event.h"
#include "vast/meta/type.h"

namespace vast {
namespace meta {

schema_manager::schema_manager()
{
  using namespace cppa;

  init_state = (
      on(atom("load"), arg_match) >> [=](std::string const& file)
      {
        schema_.reset(new taxonomy);
        schema_->load(file);
      },
      on(atom("print")) >> [=]()
      {
        reply("schema", schema_->to_string());
      });
}

} // namespace meta
} // namespace vast
