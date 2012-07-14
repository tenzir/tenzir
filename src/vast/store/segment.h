#ifndef VAST_STORE_SEGMENT_H
#define VAST_STORE_SEGMENT_H

#include <vector>
#include <string>
#include <cppa/cow_tuple.hpp>
#include <ze/forward.h>
#include <ze/object.h>
#include <ze/compression.h>
#include <ze/chunk.h>
#include <ze/serialization.h>
#include <ze/type/time.h>
#include <vast/store/exception.h>

namespace vast {
namespace store {

/// Contains a vector of chunks with additional meta data. 
class segment : public ze::object
{
public:
  typedef ze::chunk<ze::event> chunk;
  typedef cppa::cow_tuple<chunk> chunk_tuple;

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
    reader(chunk const& chunk);

    /// Deserializes an event into the segment.
    /// @param event The event to deserialize into.
    /// @return The number of events left for extraction in the current chunk.
    uint32_t operator>>(ze::event & event);

    /// Invokes a callback on each deserialized event.
    /// @param A callback taking the new event as argument.
    /// @return The number of bytes processed.
    size_t read(std::function<void(ze::event)> f);

    /// Retrieves the total number of bytes processed across all chunks.
    /// @return The number of bytes written from the input archive.
    size_t bytes() const;

  private:
    size_t bytes_ = 0;
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
  
  /// Retrieves the total number events in the segment.
  uint32_t events() const;

  /// Retrieves the number of bytes the segment occupies.
  /// @return The number of bytes the segment occupies.
  size_t bytes() const;

  /// Retrieves the number of chunks.
  /// @return The number of chunks in the segment.
  size_t size() const;

  /// Creates a reader proxy to read a given chunk.
  ///
  /// @param i The chunk index, must be in *[0, n)* where *n* is the
  /// number of chunks in `s` obtainable via segment::size().
  reader read(size_t i) const;

  /// Creates a writer proxy to read a given chunk.
  writer write();

private:
  friend bool operator==(segment const& x, segment const& y);
  friend bool operator!=(segment const& x, segment const& y);

  template <typename Archive>
  friend void save(Archive& oa, segment const& s)
  {
    oa << segment::magic;
    oa << s.version_;
    oa << static_cast<ze::object const&>(s);
    oa << s.compression_;
    oa << s.start_;
    oa << s.end_;
    oa << s.event_names_;
    oa << s.events_;

    uint32_t size = s.chunks_.size();
    oa << size;
    for (auto& tuple : s.chunks_)
      oa << cppa::get<0>(tuple);
  }

  template <typename Archive>
  void load(Archive& ia, segment& s)
  {
    uint32_t magic;
    ia >> magic;
    if (magic != segment::magic)
      throw segment_exception("invalid segment magic");

    ia >> s.version_;
    if (s.version_ > segment::version)
      throw segment_exception("segment version too high");

    ia >> static_cast<ze::object&>(s);
    ia >> s.compression_;
    ia >> s.start_;
    ia >> s.end_;
    ia >> s.event_names_;
    ia >> s.events_;

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

  uint32_t version_;
  ze::compression compression_;
  ze::time_point start_;
  ze::time_point end_;
  std::vector<std::string> event_names_;
  uint32_t events_;
  std::vector<chunk_tuple> chunks_;
};

} // namespace store
} // namespace vast

#endif
