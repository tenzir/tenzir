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

/// A sequence of chunks with additional meta data.
class segment : util::equality_comparable<segment>
{
public:
  /// Segment meta data.
  struct meta_data : util::equality_comparable<meta_data>
  {
    static uint32_t const magic = 0x2a2a2a2a;
    static uint32_t const version = 1;

    /// Checks whether the segment contains the event with the given ID.
    /// @param eid The event ID to check.
    /// @returns `true` iff the segment contains the event having id *eid*.
    bool contains(event_id eid) const;

    /// Checks whether the segment contains the given event half-open ID range.
    /// @param from The left side of the interval.
    /// @param to The right side of the interval.
    /// @returns `true` iff the segment contains *[from, to)*.
    bool contains(event_id from, event_id to) const;

    uuid id;
    time_point first = time_range{};
    time_point last = time_range{};
    event_id base = invalid_event_id;
    uint64_t events = 0;
    uint64_t bytes = 0;
    vast::schema schema;

  private:
    friend access;

    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);

    friend bool operator==(meta_data const& x, meta_data const& y);
  };

  /// A proxy class for reading from a segment. Multiple readers can safely
  /// access the same underlying segment.
  class reader
  {
  public:
    /// Constructs a reader for a specific segment.
    /// @param s The segment to read from.
    reader(segment const& s);

    /// Extracts an event from the chunk.
    /// @param id If non `invalid_event_id`, specifies the ID of the
    ///           event to extract. If `invalid_event_id`, the function
    ///           extracts the next event from the underlying chunk.
    /// @returns The extracted event, an empty result if there are no more
    ///          events available, or an error on failure.
    result<event> read(event_id id = invalid_event_id);

  private:
    chunk const* prev();
    chunk const* next();

    segment const* segment_;
    chunk const* current_ = nullptr;
    event_id next_ = 0;
    event_id chunk_base_ = invalid_event_id;
    size_t chunk_idx_ = 0;
    std::unique_ptr<chunk::reader> chunk_reader_;
  };

  /// Constructs a segment.
  /// @param id The UUID of the segment.
  segment(uuid id = uuid::random());

  /// Appends a chunk to this segment.
  /// @param chk The chunk to append.
  /// @returns `nothing` on success.
  trial<void> push_back(chunk chk);

  /// Retrieves the segment ::schema.
  /// @returns The schema of the segment.
  meta_data const& meta() const;

  /// Checks whether the segment has no chunks.
  /// @returns `true` iff `size() == 0`.
  bool empty() const;

  /// Retrieves the number of chunks.
  /// @returns The number of chunks.
  size_t size() const;

private:
  meta_data meta_;
  std::vector<chunk> chunks_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend bool operator==(segment const& x, segment const& y);
};

} // namespace vast

#endif
