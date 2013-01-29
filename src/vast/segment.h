#ifndef VAST_SEGMENT_H
#define VAST_SEGMENT_H

#include <vector>
#include <string>
#include <cppa/cow_tuple.hpp>
#include <ze/chunk.h>
#include <ze/time.h>
#include <ze/uuid.h>
#include <ze/io/compression.h>
#include <ze/util/operators.h>

namespace vast {

/// Contains a vector of chunks with additional meta data. 
class segment : ze::util::equality_comparable<segment>
{
public:
  typedef ze::chunk<ze::event> chunk;
  typedef cppa::cow_tuple<chunk> chunk_tuple;

  /// The segment header.
  struct header : ze::util::equality_comparable<header>
  {
    /// The event-related meta data inside the segment header.
    struct event_meta_data : ze::util::equality_comparable<event_meta_data>,
                             ze::util::addable<event_meta_data>
    {
      /// Default-constructs event meta data.
      event_meta_data();

      friend bool operator==(event_meta_data const& x,
                             event_meta_data const& y);

      // Merges the event meta.
      // @param other The meta data to merge..
      // @param A reference to `*this`.
      event_meta_data& operator+=(event_meta_data const& other);

      /// Integrates the meta of an event into the header.
      /// @param event The event to integrate.
      void accommodate(ze::event const& event);

      ze::time_point start;
      ze::time_point end;
      uint32_t n = 0;
    };

    uint32_t version = 0;
    ze::uuid id;
    uint64_t base = 0;
    ze::io::compression compression;
    event_meta_data event_meta;

  private:
    friend bool operator==(header const& x, header const& y);

    friend ze::io::access;
    void serialize(ze::io::serializer& sink);
    void deserialize(ze::io::deserializer& source);
  };

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
    /// @param event The event to store.
    void operator<<(ze::event const& event);

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
    header::event_meta_data event_meta_;
    chunk chunk_;
    chunk::putter putter_;
    size_t processed_bytes_ = 0;
    size_t chunk_bytes_ = 0;
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
    reader(reader&& other) = default;

    /// Tests whether the reader has more events to extract.
    /// @return `true` if the reader has more events available.
    explicit operator bool () const;

    /// Deserializes an event from the segment.
    /// @param event The event to deserialize into.
    /// @return The number of events left for extraction in the current chunk.
    void operator>>(ze::event& event);

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
    chunk::getter getter_;
    size_t processed_bytes_ = 0;
    size_t chunk_bytes_ = 0;
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
  segment(segment&& other) = default;

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

  friend ze::io::access;
  void serialize(ze::io::serializer& sink);
  void deserialize(ze::io::deserializer& source);

  static uint32_t const magic = 0x2a2a2a2a;
  static uint8_t const version = 1;

  header header_;
  std::vector<chunk_tuple> chunks_;
};

} // namespace vast

#endif
