#ifndef VAST_SEGMENT_H
#define VAST_SEGMENT_H

#include <vector>
#include <string>
#include <cppa/cow_tuple.hpp>
#include <ze/chunk.h>
#include <ze/time.h>
#include <ze/uuid.h>
#include <ze/io/compression.h>

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
    header() = default;

    void serialize(ze::io::serializer& sink);
    void deserialize(ze::io::deserializer& source);

    uint32_t version = 0;
    ze::uuid id;
    ze::io::compression compression;
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
    explicit writer(segment* s);

    /// Move-constructs a writer.
    /// @param other The other writer.
    writer(writer&& other);

    /// Retrieves the total number of bytes processed across all chunks.
    /// @return The number of bytes written to the output archive.
    size_t bytes() const;

    /// Retrieves the number of elements in the current chunk.
    size_t elements() const;

    /// Serializes an event into the segment.
    /// @param event The event to store.
    /// @return The number of events in the current chunk
    uint32_t operator<<(ze::event const& event);

    /// Moves the current chunk from the writer into the segment and creates an
    /// internal new chunk for subsequent write operations.
    void flush_chunk();

  private:
    size_t bytes_ = 0;
    segment* segment_;
    chunk chunk_;
    chunk::putter putter_;
  };

  class reader
  {
    reader(reader const&) = delete;
    reader& operator=(reader) = delete;

  public:
    /// Constructs a reader for a specific segment.
    /// @param s The segment to read from.
    explicit reader(segment const* s);

    /// Move-constructs a reader.
    /// @param other The other reader.
    reader(reader&& other);

    /// Retrieves the total number of bytes processed across all chunks.
    /// @return The number of bytes written by the input archive.
    size_t bytes() const;

    /// Retrieves the number of events available in the current chunk.
    uint32_t events() const;

    /// Retrieves the number of chunks remaining to be processed.
    size_t chunks() const;

    /// Deserializes an event from the segment.
    /// @param event The event to deserialize into.
    /// @return The number of events left for extraction in the current chunk.
    uint32_t operator>>(ze::event& event);

  private:
    size_t bytes_ = 0;
    size_t total_bytes_ = 0;
    segment const* segment_;
    std::vector<chunk_tuple>::const_iterator chunk_;
    chunk::getter getter_;
  };

  /// Constructs a segment.
  /// @param method The UUID of the segment.
  /// @param method The compression method to use for each chunk.
  segment(ze::uuid uuid = ze::uuid::nil(),
          ze::io::compression method = ze::io::lz4);

  /// Copy-constructs a segment.
  /// @param other The segment to copy.
  segment(segment const& other);

  /// Move-constructs a segment.
  /// @param other The segment to move.
  segment(segment&& other);

  /// Assigns a segment to this instance.
  /// @param other The RHS of the assignment.
  /// @return A reference to the LHS of the assignment.
  segment& operator=(segment other);

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
