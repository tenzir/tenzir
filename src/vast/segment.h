#ifndef VAST_SEGMENT_H
#define VAST_SEGMENT_H

#include <vector>
#include <string>
#include <cppa/cow_tuple.hpp>
#include <ze/forward.h>
#include <ze/uuid.h>
#include <ze/compression.h>
#include <ze/chunk.h>
#include <ze/serialization.h>
#include <ze/type/time.h>
#include "vast/exception.h"

namespace vast {

/// Contains a vector of chunks with additional meta data. 
class segment
{
public:
  typedef ze::chunk<ze::event> chunk;
  typedef cppa::cow_tuple<chunk> chunk_tuple;

  /// The segment header.
  struct header
  {
    header();

    template <typename Archive>
    friend void serialize(Archive& oa, header const& h)
    {
      oa << segment::magic;
      oa << h.version;
      oa << h.id;
      oa << h.compression;
      oa << h.start;
      oa << h.end;
      oa << h.event_names;
      oa << h.events;
    }

    template <typename Archive>
    friend void deserialize(Archive& ia, header& h)
    {
      uint32_t magic;
      ia >> magic;
      if (magic != segment::magic)
        throw error::segment("invalid segment magic");

      ia >> h.version;
      if (h.version > segment::version)
        throw error::segment("segment version too high");

      ia >> h.id;
      ia >> h.compression;
      ia >> h.start;
      ia >> h.end;
      ia >> h.event_names;
      ia >> h.events;
    }

    uint32_t version = 0;
    ze::uuid id;
    ze::compression compression;
    ze::time_point start;
    ze::time_point end;
    std::vector<std::string> event_names;
    uint32_t events = 0;
  };

  class writer
  {
    writer(writer const&) = delete;
    writer& operator=(writer) = delete;

  public:
    /// Creates a new chunk at the end of the segment for writing.
    /// @param s The segment to write to.
    writer(segment& s);

    /// Moves the current chunk from the writer into the segment and creates an
    /// internal new chunk for subsequent write operations.
    void flush_chunk();

    /// Serializes an event into the segment.
    /// @param event The event to store.
    /// @return The number of events in the current chunk
    uint32_t operator<<(ze::event const& event);

    /// Retrieves the total number of bytes processed across all chunks.
    /// @return The number of bytes written to the output archive.
    size_t bytes() const;

    /// Retrieves the number of elements in the current chunk.
    size_t elements() const;

  private:
    size_t bytes_ = 0;
    segment& segment_;
    chunk chunk_;
    chunk::putter putter_;
  };

  class reader
  {
    reader(reader const&) = delete;
    reader& operator=(reader) = delete;

  public:
    /// Creates a reader for a specific segment chunk.
    ///
    /// @param s The segment to read from.
    ///
    /// @param i The chunk index, must be in *[0, n)* where *n* is the
    /// number of chunks in `s`.
    reader(segment const& s);

    /// Deserializes an event into the segment.
    /// @param event The event to deserialize into.
    /// @return The number of events left for extraction in the current chunk.
    uint32_t operator>>(ze::event& event);

    /// Retrieves the total number of bytes processed across all chunks.
    /// @return The number of bytes written from the input archive.
    size_t bytes() const;

    /// Retrieves the number of events available in the current chunk
    uint32_t events() const;

    /// Retrieves the number of chunks remaining to be processed.
    size_t chunks() const;

  private:
    size_t bytes_ = 0;
    size_t total_bytes_ = 0;
    segment const& segment_;
    std::vector<chunk_tuple>::const_iterator chunk_;
    chunk::getter getter_;
  };

  /// Constructs a segment.
  /// @param method The compression method to use for each chunk.
  segment(ze::compression method = ze::compression::none);

  /// Retrieves a const-reference to a chunk tuple.
  ///
  /// @param i The chunk index, must be in *[0, n)* where *n* is the
  /// number of chunks in `s` obtainable via segment::size().
  chunk_tuple operator[](size_t i) const;
  
  /// Retrieves the segment header for inspection.
  header const& head() const;

  /// Retrieves the number of events in the segment.
  uint32_t events() const;

  /// Retrieves the number of bytes the segment occupies.
  /// @return The number of bytes the segment occupies.
  size_t bytes() const;

  /// Retrieves the number of chunks.
  /// @return The number of chunks in the segment.
  size_t size() const;

  /// Retrieves the segment ID.
  /// @return A UUID identifying the segment.
  ze::uuid const& id() const;

private:
  friend bool operator==(segment const& x, segment const& y);
  friend bool operator!=(segment const& x, segment const& y);

  template <typename Archive>
  friend void serialize(Archive& oa, segment const& s)
  {
    oa << s.header_;

    uint32_t size = s.chunks_.size();
    oa << size;
    for (auto& tuple : s.chunks_)
      oa << cppa::get<0>(tuple);
  }

  template <typename Archive>
  friend void deserialize(Archive& ia, segment& s)
  {
    ia >> s.header_;

    uint32_t n;
    ia >> n;
    s.chunks_.resize(n);
    for (auto& tuple : s.chunks_)
    {
      chunk chk;
      ia >> chk;
      auto t = cppa::make_cow_tuple(std::move(chk));
      tuple = std::move(t);
    }
  }

  static uint32_t const magic = 0x2a2a2a2a;
  static uint8_t const version = 1;

  header header_;
  std::vector<chunk_tuple> chunks_;
};

} // namespace vast

#endif
