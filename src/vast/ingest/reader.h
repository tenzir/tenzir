#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <iosfwd>
#include <ze/actor.h>
#include <ze/vertex.h>
#include "vast/fs/fstream.h"
#include "vast/fs/path.h"
#include "vast/comm/event_source.h"
#include "vast/ingest/bro-1.5/conn.h"
#include "vast/util/parser/streamer.h"

namespace vast {
namespace ingest {

/// A reader that transforms file contents into events.
class reader : public ze::publisher<>
             , public ze::actor<reader>
{
    friend class ze::actor<reader>;
    reader(reader const&) = delete;

public:
    /// Constructs a reader.
    /// @param c The component the reader belongs to.
    /// @param filename The file to ingest.
    reader(ze::component& c, fs::path const& filename);
    virtual ~reader() = default;

protected:
    /// Parses a single event from the underlying filestream.
    /// @param event The resulting event from a single parse operation.
    /// @return `true` *iff* parsing succeeded.
    virtual bool parse(ze::event& event) = 0;

    /// Determines whether the reader finished parsing.
    virtual bool done() = 0;

    fs::ifstream file_;

private:
    void act();

    size_t const batch_size_ = 10000;
    size_t events_ = 0;
};


/// A reader for Bro log files.
class bro_reader : public reader
{
    typedef util::parser::streamer<
        ingest::bro15::parser::connection
      , ingest::bro15::parser::skipper
      , ze::event
    > streamer;

public:
    bro_reader(ze::component& c, fs::path const& filename);

protected:
    virtual bool parse(ze::event& event);
    virtual bool done();

private:
    streamer streamer_;
};

} // namespace ingest
} // namespace vast

#endif
