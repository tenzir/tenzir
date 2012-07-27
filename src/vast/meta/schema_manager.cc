#include "vast/meta/schema_manager.h"

#include "vast/meta/taxonomy.h"
#include "vast/meta/event.h"
#include "vast/meta/type.h"
#include "vast/logger.h"

namespace vast {
namespace meta {

schema_manager::schema_manager()
{
  LOG(verbose, meta) << "spawning schema manager @" << id();
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
      },
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, meta) << "schema manager @" << id() << " terminated";
      });
}

} // namespace meta
} // namespace vast
