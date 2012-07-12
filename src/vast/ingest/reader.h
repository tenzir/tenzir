#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <iosfwd>
#include <cppa/cppa.hpp>

namespace vast {
namespace ingest {

/// A reader that transforms file contents into events.
class reader : cppa::sb_actor<reader>
{
public:
  /// Constructs a reader.
  /// @param upstream The upstream actor receiving the events.
  reader(cppa::actor_ptr upstream);
  virtual ~reader() = default;

  cppa::behavior init_state;

protected:
  /// Extracts events from a filestream.
  /// @param ifs The file stream to extract events from.
  /// @return The number of events extracted.
  virtual size_t extract(std::ifstream& ifs) = 0;

  cppa::actor_ptr upstream_;
};


/// A reader for Bro log files.
class bro_reader : public reader
{
public:
  bro_reader(cppa::actor_ptr upstream);

protected:
  virtual size_t extract(std::ifstream& ifs);
};

} // namespace ingest
} // namespace vast

#endif
