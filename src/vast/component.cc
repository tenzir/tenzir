#include "vast/component.h"

#include "vast/util/logger.h"

namespace vast {

emit_component::emit_component(ze::io& io)
  : ze::component<ze::event>(io)
  , loader(*this)
{
}

ingest_component::ingest_component(ze::io& io)
  : ze::component<ze::event>(io)
  , source(*this)
  , archiver(*this)
{
    link(source, archiver);
}

query_component::query_component(ze::io& io)
  : ze::component<ze::event>(io)
  , processor(*this)
{
}

} // namespace vast
