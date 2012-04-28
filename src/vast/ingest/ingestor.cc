#include "vast/ingest/ingestor.h"

#include "vast/ingest/reader.h"
#include "vast/util/logger.h"

namespace vast {
namespace ingest {

ingestor::ingestor(ze::io& io)
  : ze::component(io)
  , source(*this)
{
}

void ingestor::stop()
{
    LOG(debug, ingest) << "stopping source";
    source.stop();

    LOG(debug, ingest) << "stopping readers";
    for (auto& r : readers_)
        r->stop();

    readers_.clear();
}

std::shared_ptr<reader> ingestor::make_reader(fs::path const& filename)
{
    // TODO: Support multiple readers that detect the file format dynamically.
    auto r = std::make_shared<bro_reader>(*this, filename);
    readers_.push_back(r);
    return r;
}

} // namespace ingest
} // namespace vast
