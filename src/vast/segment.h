#ifndef VAST_SEGMENT_H
#define VAST_SEGMENT_H

#include <vector>
#include <string>
#include "vast/aliases.h"
#include "vast/chunk.h"
#include "vast/cow.h"
#include "vast/time.h"
#include "vast/optional.h"
#include "vast/uuid.h"
#include "vast/io/compression.h"
#include "vast/util/operators.h"

namespace vast {

class event;

/// Contains a vector of chunks with additional meta data.
class segment : util::equality_comparable<segment>
{
public:
  /// A proxy class for writing into a segment. Each writer maintains a local
  /// chunk that receives events to serialize. Upon flushing, the writer
  /// appends the chunk to the underlying segment.
  class writer
  {
  public:
    /// Constructs a writer to serialize events into a segment.
    ///
    /// @param s The segment to write to.
    ///
    /// @param max_events_per_chunk The maximum number of events per chunk. If
    /// non-zero and *max_segment_size* has not yet been reached, the writer
    /// will create a new chunk
    ///
    /// @pre `s != nullptr`
    explicit writer(segment* s, size_t max_segment_size = 0);

    /// Destructs a writer and flushes the event chunk into the underlying
    /// segment.
    ///
    /// @warning If the segment has no more room (only if the
    /// `max_segment_size` parameter given at construction time was non-zero),
    /// flushing may fail and events may get lost.
    ~writer();

    /// Serializes an event into the underlying segment.
    /// @param e The event to write.
    /// @returns `true` on success and `false` if the segment is full.
    bool write(event const& e);

    /// Attaches the writer to a new segment.
    /// @param s The segment to attach the writer to.
    /// @pre `s != nullptr`
    void attach_to(segment* s);

    /// Seals the current chunk and appends it to the list of chunks in the
    /// underlying segment.
    ///
    /// @returns `false` on failure, `true` on success or if there
    /// were no events to flush.
    bool flush();

    /// Tests whether the underlying segment is full, i.e., can no longer take
    /// chunks because they would exceed the maximum segment size provided at
    /// construction time of this writer.
    ///
    /// @returns `true` *iff* the writer has a maximum segment size and the
    /// current chunk exceeds it.
    bool full() const;

    /// Retrieves the number of bytes processed in total.
    /// @returns The number of bytes written into this writer.
    size_t bytes() const;

  private:
    bool store(event const& e);

    segment* segment_;
    std::unique_ptr<chunk> chunk_;
    std::unique_ptr<chunk::writer> writer_;
    size_t max_events_per_chunk_;
  };

  /// A proxy class for reading from a segment. Multiple readers can safely
  /// access the same underlying segment.
  class reader
  {
  public:
    /// Constructs a reader for a specific segment.
    /// @param s The segment to read from.
    explicit reader(segment const* s);

    /// Deserializes an event from the segment.
    /// @param e The event to deserialize into.
    /// @returns The number of events left for extraction in the current chunk.
    bool read(event& e);

    /// Fast-forwards the reader an event with a given ID.
    /// @param The event ID to forward to.
    /// @returns `true` if skipping to *id* succeeded.
    /// @post The next call to ::read exctracts the event with ID *id*.
    bool seek(event_id id);

    /// Checks whether the reader has still events that can be read.
    /// @returns `true` iff the reader is empty.
    bool empty() const;

  private:
    bool read(event* e);
    bool load(event* e);

    segment const* segment_;
    std::unique_ptr<chunk::reader> reader_;
    size_t next_ = 0;
    event_id current_id_ = 0;
  };

  static uint32_t const magic = 0x2a2a2a2a;
  static uint8_t const version = 1;

  /// Constructs a segment.
  /// @param id The UUID of the segment.
  /// @param max_bytes The maximum segment size.
  /// @param method The compression method to use for each chunk.
  segment(uuid id = uuid::nil(), size_t max_bytes = 0,
          io::compression method = io::lz4);

  /// Retrieves the segment ID.
  /// @returns A UUID identifying the segment.
  uuid const& id() const;

  /// Sets the segment base ID for events.
  /// @param id The base event ID for this segment.
  void base(event_id id);

  /// Retrieves the segment base ID for events.
  /// @returns The base event ID for this segment.
  event_id base() const;

  /// Checks whether the segment contains the event with the given ID.
  /// @param eid The event ID to check.
  /// @returns `true` iff the segment contains the event having id *eid*.
  bool contains(event_id eid) const;

  /// Retrieves the number of events in the segment.
  uint32_t events() const;

  /// Retrieves the number of bytes the segment occupies in memory.
  uint32_t bytes() const;

  /// Retrieves the maximum number of bytes this segment can occupy.
  ///
  /// @returns The maximum number of bytes this segment can occupy or 0 if its
  /// size is unbounded.
  size_t max_bytes() const;

  /// Writes a vector of events into the segment.
  /// @param v The vector of events to write.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @returns The number of events successfully written.
  size_t store(std::vector<event> const& v, size_t max_events_per_chunk = 0);

  /// Extracts a single event with a given ID.
  /// @param id The ID of the event.
  /// @returns The event having ID *id* or an disengaged option otherwise.
  optional<event> load(event_id id) const;

private:
  bool append(chunk& c);

  uuid id_;
  io::compression compression_;
  event_id base_ = 0;
  uint32_t n_ = 0;
  uint32_t max_bytes_ = 0;
  uint32_t occupied_bytes_ = 0;
  std::vector<cow<chunk>> chunks_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend bool operator==(segment const& x, segment const& y);
};

} // namespace vast

#endif
