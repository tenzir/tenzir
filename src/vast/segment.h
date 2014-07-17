#ifndef VAST_SEGMENT_H
#define VAST_SEGMENT_H

#include <list>
#include <string>
#include <vector>
#include "vast/aliases.h"
#include "vast/chunk.h"
#include "vast/time.h"
#include "vast/optional.h"
#include "vast/schema.h"
#include "vast/uuid.h"
#include "vast/io/compression.h"
#include "vast/util/operators.h"
#include "vast/util/result.h"

namespace vast {

class event;

/// Contains a vector of chunks with additional meta data.
class segment : util::equality_comparable<segment>
{
public:
  /// Segment meta data.
  struct header : util::equality_comparable<header>
  {
    static uint32_t const magic = 0x2a2a2a2a;
    static uint32_t const version = 1;

    uuid id;
    io::compression compression;
    time_point first = time_range{};
    time_point last = time_range{};
    event_id base = 0;
    uint64_t n = 0;
    uint64_t max_bytes = 0;
    uint64_t occupied_bytes = 0;
    vast::schema schema;

  private:
    friend access;

    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);

    friend bool operator==(header const& x, header const& y);
  };

  /// A proxy class for writing into a segment. Each writer maintains a local
  /// chunk that receives events to serialize. Upon flushing, the writer
  /// appends the chunk to the underlying segment. Consequently,
  /// multiple writers may exist simultaneously.
  class writer
  {
  public:
    /// Constructs a writer to serialize events into a segment.
    /// @param s The segment to write to.
    /// @param max_events_per_chunk The maximum number of events per chunk.
    /// @pre `s != nullptr`
    explicit writer(segment* s, size_t max_events_per_chunk = 0);

    /// Destructs a writer and flushes the event chunk into the underlying
    /// segment.
    ///
    /// @warning If the segment has no more room, flushing may fail and events
    /// may get lost.
    ~writer();

    /// Serializes an event into the writer. If the event type is not
    /// [invalid](::invalid_type), then the writer adds the type to its local
    /// schema, which gets merged with the segment-wide schema upon flushing.
    ///
    /// @param e The event to write.
    ///
    /// @returns `true` on success and `false` if the writer is full.
    bool write(event const& e);

    /// Attaches the writer to a new segment.
    /// @param s The segment to attach the writer to.
    /// @pre `s != nullptr`
    void attach_to(segment* s);

    /// Seals the current chunk and appends it to the list of chunks in the
    /// underlying segment.
    ///
    /// @returns `nothing` on success or if there were no events to flush.
    trial<void> flush();

    /// Retrieves the number of bytes processed in total.
    /// @returns The number of bytes written into this writer.
    size_t bytes() const;

  private:
    bool store(event const& e);

    segment* segment_;
    std::unique_ptr<chunk> chunk_;
    std::unique_ptr<chunk::writer> chunk_writer_;
    schema schema_;
    size_t max_events_per_chunk_;
    time_point first_ = time_range{};
    time_point last_ = time_range{};
  };

  /// A proxy class for reading from a segment. Multiple readers can safely
  /// access the same underlying segment.
  class reader
  {
  public:
    /// Constructs a reader for a specific segment.
    /// @param s The segment to read from.
    explicit reader(segment const* s);

    /// Retrieves the current position of the reader.
    /// @returns The ID of the next event to ::read.
    event_id position() const;

    /// Reads the next event from the current position.
    /// @param id If non-zero, specifies the ID of the event to extract.
    /// @returns The extracted event on success.
    trial<event> read(event_id id = 0);

    /// Seeks to an event with a given ID.
    ///
    /// @param id The event ID to seek to.
    ///
    /// @returns `true` if seeking to *id* succeeded, and `false` if *id* is
    /// out-of-bounds.
    ///
    /// @post The next call to ::read exctracts the event with ID *id*.
    bool seek(event_id id);

  private:
    /// Extracts events according to given boundaries.
    ///
    /// @param from The ID where to start extraction. If 0, will use the
    /// current position of the reader.
    ///
    /// @param to The ID where to end extraction. If 0, will extract until the
    /// end of the current chunk.
    ///
    /// @param f The function to invoke on each extracted event.
    ///
    /// @returns An engaged value with the number of times *f* has been
    /// applied, and a disengaged value if an error occurred.
    optional<size_t> extract(event_id from,
                             event_id to,
                             std::function<void(event)> f);

    /// Moves to the next chunk.
    /// @returns A pointer to the next chunk or `nullptr` on failure.
    chunk const* next();

    /// Moves to the previous chunk.
    /// @returns A pointer to the previous chunk or `nullptr` on failure.
    chunk const* prev();

    /// Resets the internal reading position to the beginning of the current
    /// chunk.
    ///
    /// @returns The number of events backed up.
    event_id backup();

    /// Skips over a given number of events.
    /// @param n The number of events to skip.
    /// @returns The number of events skipped on success.
    trial<event_id> skip(size_t n);

    /// Loads the next event.
    ///
    /// @param discard Flag indicating whether to discard or return the
    /// deserialized event.
    ///
    /// @returns An event if *discard* was `false` and an empty result if
    /// *discard* was `true`.
    result<event> load(bool discard = false);

    /// Checks whether a given ID falls into the current chunk.
    /// @param eid The event ID to check.
    /// @returns `true` if *eid* falls into the current chunk.
    bool within_current_chunk(event_id eid) const;

    segment const& segment_;
    chunk const* current_ = nullptr;
    event_id next_ = 0;
    event_id chunk_base_ = 0;
    size_t chunk_idx_ = 0;
    std::unique_ptr<chunk::reader> chunk_reader_;
  };

  /// Constructs a segment.
  /// @param id The UUID of the segment.
  /// @param max_bytes The maximum segment size.
  /// @param method The compression method to use for each chunk.
  segment(uuid id = uuid::nil(), uint64_t max_bytes = 0,
          io::compression method = io::lz4);

  /// Sets the segment base ID for events.
  /// @param id The base event ID for this segment.
  void base(event_id id);

  /// Writes a vector of events into the segment.
  /// @param v The vector of events to write.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @returns The number of events successfully written.
  size_t store(std::vector<event> const& v, size_t max_events_per_chunk = 0);

  /// Writes a sequence of events into the segment.
  /// @param begin The start iterator.
  /// @param end The one-past-the-end iterator.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @returns The number of events successfully written.
  template <typename Iterator>
  size_t store(Iterator begin, Iterator end, size_t max_events_per_chunk = 0)
  {
    writer w(this, max_events_per_chunk);

    size_t n;
    while (begin != end)
    {
      if (! w.write(*begin++))
        break;
      ++n;
    }

    return n;
  }

  /// Extracts a single event with a given ID.
  /// @param id The ID of the event.
  /// @returns The event having ID *id* on success
  trial<event> load(event_id id) const;

  /// Retrieves the segment ID.
  /// @returns A UUID identifying the segment.
  uuid const& id() const;

  /// Retrieves the timestamp of the earliest event in the segment.
  time_point first() const;

  /// Retrieves the timestamp of the oldest event in the segment.
  time_point last() const;

  /// Retrieves the segment base ID for events.
  /// @returns The base event ID for this segment.
  event_id base() const;

  /// Checks whether the segment contains the event with the given ID.
  /// @param eid The event ID to check.
  /// @returns `true` iff the segment contains the event having id *eid*.
  bool contains(event_id eid) const;

  /// Checks whether the segment contains the given event half-open ID range.
  /// @param from The left side of the interval.
  /// @param to The right side of the interval.
  /// @returns `true` iff the segment contains *[from, to]*.
  bool contains(event_id from, event_id to) const;

  /// Retrieves the number of events in the segment.
  uint64_t events() const;

  /// Generates a bitstream representing the segment coverage based on the
  /// contained event IDs.
  template <typename Bitstream = default_bitstream>
  optional<Bitstream> coverage() const
  {
    if (base() == 0 || events() == 0)
      return {};

    Bitstream bs;
    bs.append(base(), false);
    bs.append(events(), true);

    return bs;
  }

  /// Retrieves the number of bytes the segment occupies in memory.
  uint64_t bytes() const;

  /// Retrieves the maximum number of bytes this segment can occupy.
  ///
  /// @returns The maximum number of bytes this segment can occupy or 0 if its
  /// size is unbounded.
  uint64_t max_bytes() const;

  /// Retrieves the segment ::schema.
  /// @returns The schema of the segment.
  vast::schema const& schema() const;

private:
  header header_;
  std::vector<chunk> chunks_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend bool operator==(segment const& x, segment const& y);
};

} // namespace vast

#endif
