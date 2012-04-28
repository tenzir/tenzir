#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <iosfwd>
#include <ze/actor.h>
#include <ze/vertex.h>
#include "vast/fs/path.h"
#include "vast/comm/event_source.h"

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
    reader(ze::component& c, fs::path filename);
    virtual ~reader() = default;

protected:
    virtual bool parse(std::istream& in) = 0;

private:
    void act();

    fs::path const filename_;
};


/// A reader for Bro log files.
class bro_reader : public reader
{
public:
    bro_reader(ze::component& c, fs::path filename);
    virtual ~bro_reader();

protected:
    virtual bool parse(std::istream& in);
};

} // namespace ingest
} // namespace vast

#endif
