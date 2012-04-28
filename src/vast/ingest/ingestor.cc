#include "vast/ingest/ingestor.h"

#include "vast/ingest/reader.h"

namespace vast {
namespace ingest {

ingestor::ingestor(ze::io& io)
  : ze::component(io)
  , source(*this)
{
}

std::shared_ptr<reader> ingestor::make_reader(fs::path const& filename)
{
    // TODO: Support multiple readers that detect the file format dynamically.
    return std::make_shared<bro_reader>(*this, filename);
}

} // namespace ingest
} // namespace vast
