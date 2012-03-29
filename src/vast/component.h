#ifndef VAST_COMPONENT_H
#define VAST_COMPONENT_H

#include "vast/query/processor.h"
#include "vast/store/archiver.h"
#include "vast/store/loader.h"
#include "vast/comm/event_source.h"

namespace vast {

/// The emit_component component.
struct emit_component : public ze::component<ze::event>
{
    emit_component(ze::io& io);
    emit_component(emit_component const&) = delete;
    emit_component& operator=(emit_component const&) = delete;

    store::loader loader;
};

/// The ingestion component.
struct ingest_component : public ze::component<ze::event>
{
    ingest_component(ze::io& io);
    ingest_component(ingest_component const&) = delete;
    ingest_component& operator=(ingest_component const&) = delete;

    comm::event_source source;
    store::archiver archiver;
};

struct query_component : public ze::component<ze::event>
{
    query_component(ze::io& io);
    query_component(query_component const&) = delete;
    query_component& operator=(query_component const&) = delete;

    query::processor processor;
};

} // namespace vast

#endif
