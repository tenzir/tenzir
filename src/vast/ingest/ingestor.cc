#include "vast/ingest/ingestor.h"

namespace vast {
namespace ingest {

ingestor::ingestor(ze::io& io)
  : ze::component(io)
  , source(*this)
{
}

} // namespace ingest
} // namespace vast
