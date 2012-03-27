#include "vast/store/ingestion.h"

namespace vast {
namespace store {

ingestion::ingestion(ze::io& io)
  : comm::event_component(io)
  , event_source_(*this)
  , archiver_(*this)
{
    link(event_source_, archiver_);
}

void ingestion::init(std::string const& ip,
                     unsigned port,
                     fs::path const& directory,
                     size_t max_chunk_events,
                     size_t max_segment_size)
{
    event_source_.init(ip, port);
    // FIXME: debugging only.
    event_source_.subscribe("new_connection");
    event_source_.subscribe("http_header");
    event_source_.subscribe("http_request");
    event_source_.subscribe("http_reply");

    archiver_.init(directory, max_chunk_events, max_segment_size);
}

void ingestion::stop()
{
    event_source_.stop();
}

} // namespace store
} // namespace vast
