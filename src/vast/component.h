#ifndef VAST_COMPONENT_H
#define VAST_COMPONENT_H

#include "vast/store/archiver.h"
#include "vast/store/loader.h"
#include "vast/comm/event_source.h"

namespace vast {

/// The emit component.
struct emit : public ze::component<ze::event>
{
    emit(emit const&) = delete;
    emit& operator=(emit const&) = delete;

    emit(ze::io& io);
    void init(fs::path const& directory);
    void run();

    store::loader loader;
};

/// The ingestion component.
struct ingest : public ze::component<ze::event>
{
    ingest(ingest const&) = delete;
    ingest& operator=(ingest const&) = delete;

    ingest(ze::io& io);
    void init(std::string const& ip,
              unsigned port,
              std::vector<std::string> const& events,
              fs::path const& directory,
              size_t max_chunk_events,
              size_t max_segment_size);
    void stop();

    comm::event_source source;
    store::archiver archiver;
};

//struct query : public ze::component<ze::event>
//{
//    query(query const&) = delete;
//    query& operator=(query const&) = delete;
//
//    query(ze::io& io);
//
//    query::streamer streamer;
//};

} // namespace vast

#endif
