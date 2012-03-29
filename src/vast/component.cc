#include "vast/component.h"

#include "vast/util/logger.h"

namespace vast {

emit::emit(ze::io& io)
  : ze::component<ze::event>(io)
  , loader(*this)
{
}

void emit::init(fs::path const& directory)
{
    loader.init(directory);
}

void emit::run()
{
    loader.run();
}


ingest::ingest(ze::io& io)
  : ze::component<ze::event>(io)
  , source(*this)
  , archiver(*this)
{
    link(source, archiver);
}

void ingest::init(std::string const& ip,
                  unsigned port,
                  std::vector<std::string> const& events,
                  fs::path const& directory,
                  size_t max_chunk_events,
                  size_t max_segment_size)
{
    source.init(ip, port);
    for (const auto& event : events)
    {
        LOG(verbose, store) << "subscribing to event " << event;
        source.subscribe(event);
    }

    archiver.init(directory, max_chunk_events, max_segment_size);
}

void ingest::stop()
{
    source.stop();
}

} // namespace vast
