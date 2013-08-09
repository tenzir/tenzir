#ifndef VAST_SEGMENT_H
#define VAST_SEGMENT_H

#include <vector>
#include <string>
#include <cppa/cow_tuple.hpp>
#include "vast/chunk.h"
#include "vast/time.h"
#include "vast/uuid.h"
#include "vast/io/compression.h"
#include "vast/util/operators.h"

namespace vast {

class event;


/// The segment header.
struct segment_header : util::equality_comparable<segment_header>
{
  /// Default-constructs a header.
  segment_header();

  friend bool operator==(segment_header const& x, segment_header const& y);

  /// Integrates event meta data into the segment header.
  /// @param e The event to integrate.

  uuid id;
  io::compression compression;
  time_point start;
  time_point end;
  uint64_t base = 0;
  uint32_t n = 0;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);
};

/// Contains a vector of chunks with additional meta data.
class segment : util::equality_comparable<segment>
{
public:
  typedef chunk<event> chunk_type;
  typedef cppa::cow_tuple<chunk_type> chunk_tuple;

  /// A proxy class for writing into a segment. Each writer maintains a local
  /// chunk that receives events to serialize. Upon flushing, the writer
  /// appends the chunk to the segment.
  ///
  /// @note It is possible to have multiple writers per segment, however, the
  /// user must ensure that no call to writer::flush() occurrs concurrently.
  /// This interface is not ideal and will probably change in the future.
  class writer
  {
    writer(writer const&) = delete;
    writer& operator=(writer) = delete;

  public:
    /// Constructs a writer that writes to a segment.
    /// @param s The segment to write to.
    explicit writer(segment* s);

    /// Move-constructs a writer.
    /// @param other The other writer.
    writer(writer&& other) = default;

    /// Serializes an event into the segment.
    /// @param e The event to store.
    void operator<<(event const& e);

    /// Moves the current chunk from the writer into the segment and creates an
    /// internal new chunk for subsequent write operations.
    /// @return `true` if flushing was not a no-op.
    bool flush();

    /// Retrieves the number of elements in the current chunk.
    size_t elements() const;

    /// Retrieves the total number of bytes processed across all chunks.
    /// Updated with each call to operator<<.
    /// @return The number of bytes written into this writer.
    size_t processed_bytes() const;

    /// Retrieves the sum in bytes of all chunk sizes, excluding the current
    /// chunk. Updated with each call to `flush`.
    /// @return The number of bytes written into this writer.
    size_t chunk_bytes() const;

  private:
    segment* segment_;
    chunk_type chunk_;
    chunk_type::putter putter_;
    size_t processed_bytes_ = 0;
    size_t chunk_bytes_ = 0;
    time_point start_;
    time_point end_;
    uint32_t n_ = 0;
  };

  /// A proxy class for reading from a segment. Multiple readers can safely
  /// access the same underlying segment.
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
    reader(reader&& other) = default;

    /// Tests whether the reader has more events to extract.
    /// @return `true` if the reader has more events available.
    explicit operator bool () const;

    /// Deserializes an event from the segment.
    /// @param e The event to deserialize into.
    /// @return The number of events left for extraction in the current chunk.
    void operator>>(event& e);

    /// Retrieves the number of events available in the current chunk.
    uint32_t available_events() const;

    /// Retrieves the number of chunks remaining to be processed.
    size_t available_chunks() const;

    /// Retrieves the total number of bytes processed across all chunks.
    /// Updated with each call to operator>>.
    /// @return The number of bytes written into this writer.
    size_t processed_bytes() const;

    /// Retrieves the sum in bytes of all chunk sizes, excluding the current
    /// chunk. Updated with each chunk rotation.
    /// @return The number of bytes written into this writer.
    size_t chunk_bytes() const;

  private:
    segment const* segment_;
    std::vector<chunk_tuple>::const_iterator chunk_;
    chunk_type::getter getter_;
    size_t processed_bytes_ = 0;
    size_t chunk_bytes_ = 0;
  };

  static uint32_t const magic = 0x2a2a2a2a;
  static uint8_t const version = 1;

  /// Constructs a segment.
  /// @param method The UUID of the segment.
  /// @param method The compression method to use for each chunk.
  segment(uuid id = uuid::nil(), io::compression method = io::lz4);

  /// Copy-constructs a segment.
  /// @param other The segment to copy.
  segment(segment const& other);

  /// Move-constructs a segment.
  /// @param other The segment to move.
  segment(segment&& other) = default;

  /// Assigns a segment to this instance.
  /// @param other The RHS of the assignment.
  /// @return A reference to the LHS of the assignment.
  segment& operator=(segment other);

  /// Retrieves a const-reference to a chunk tuple.
  ///
  /// @param i The chunk index, must be in *[0, n)* where *n* is the
  /// number of chunks in `s` obtainable via segment::size().
  ///
  /// @pre `! chunks_.empty() && i < chunks_.size()`
  chunk_tuple operator[](size_t i) const;

  /// Retrieves the segment header for modification.
  /// @return A mutable reference to the segment header.
  segment_header& header();

  /// Retrieves the segment header for inspection.
  /// @return A const reference to the segment header.
  segment_header const& header() const;

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
  uuid const& id() const;

private:
  friend bool operator==(segment const& x, segment const& y);

  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  segment_header header_;
  std::vector<chunk_tuple> chunks_;
};

} // namespace vast

#endif
