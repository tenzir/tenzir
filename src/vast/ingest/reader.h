#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <iosfwd>
#include <cppa/cppa.hpp>

namespace vast {
namespace ingest {

/// A reader that transforms file contents into events.
class reader : public cppa::sb_actor<reader>
{
  friend class cppa::sb_actor<reader>;

public:
  /// Constructs a reader.
  /// @param upstream The upstream actor receiving the events.
  reader(cppa::actor_ptr upstream);
  virtual ~reader() = default;

protected:
  /// Extracts events from a filestream.
  /// @param ifs The file stream to extract events from.
  /// @param n  The number of events extracted.
  /// @return `true` *iff* the reader successfully ingested the file.
  virtual bool extract(std::ifstream& ifs, size_t& n) = 0;

  cppa::actor_ptr upstream_;
  cppa::behavior init_state;
};


/// A reader for Bro log files.
class bro_reader : public reader
{
public:
  bro_reader(cppa::actor_ptr upstream);

protected:
  virtual bool extract(std::ifstream& ifs, size_t& n);
};

} // namespace ingest
} // namespace vast

#endif
