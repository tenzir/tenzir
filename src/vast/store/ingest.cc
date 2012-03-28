#include "vast/store/ingest.h"

#include "vast/util/logger.h"

namespace vast {
namespace store {

ingest::ingest(ze::io& io)
  : comm::event_component(io)
  , event_source_(*this)
  , archiver_(*this)
{
    link(event_source_, archiver_);
}

void ingest::init(std::string const& ip,
                     unsigned port,
                     std::vector<std::string> const& events,
                     fs::path const& directory,
                     size_t max_chunk_events,
                     size_t max_segment_size)
{
    event_source_.init(ip, port);
    for (const auto& event : events)
    {
        LOG(verbose, store) << "subscribing to event " << event;
        event_source_.subscribe(event);
    }

    archiver_.init(directory, max_chunk_events, max_segment_size);
}

void ingest::stop()
{
    event_source_.stop();
}

} // namespace store
} // namespace vast
