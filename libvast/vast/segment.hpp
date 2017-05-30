#ifndef VAST_SEGMENT_HPP
#define VAST_SEGMENT_HPP

#include <cstdint>
#include <memory>

#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/filesystem.hpp"
#include "vast/layout.hpp"

#include "vast/detail/mmapbuf.hpp"

namespace vast {

class bitmap;
class type;

/// Enables incremental building of a segment from a sequence of events.
class segment_builder {
  segment_builder(const segment_builder&) = delete;
  segment_builder& operator=(const segment_builder&) = delete;

public:
  /// Default maximum segment size.
  static constexpr size_t default_size = 128 << 20;

  /// Constructs a segment builder with in-memory memory mapping.
  /// @param size The size of the mapped memory region.
  explicit segment_builder(size_t size = default_size);

  /// Constructs a segment builder with a file-backed memory mapping.
  /// @param filename The location of the segment on the filesystem.
  /// @param size The size of the mapped memory region.
  explicit segment_builder(const path& filename, size_t size = default_size);

  segment_builder(segment_builder&&) = default;
  segment_builder& operator=(segment_builder&&) = default;

  /// Appends an event to the builder.
  /// @param e The event to append.
  /// @returns An error iff the operation failed.
  expected<void> put(const event& e);

  /// Retrieves an event that has been added to the builder.
  /// @param id The ID of the event to get.
  /// @returns The event with ID *id* or error if no such event exists.
  expected<event> get(event_id id) const;

  /// Retrieves a set of events that have been added to the builder.
  /// @param id The ID of the event to get.
  /// @returns The event with ID *id* or error if no such event exists.
  expected<std::vector<event>> get(const bitmap& ids) const;

  /// Finalizes creation of the segment by appending accumulated meta data at
  /// the end.
  /// @returns The chunk representing a segment on success.
  expected<chunk_ptr> finish();

  /// @returns The number of bytes written so far.
  friend uint64_t bytes(const segment_builder& b);

private:
  expected<event> extract(size_t offset) const;

  std::unique_ptr<detail::mmapbuf> streambuf_;
  std::unique_ptr<layout::writer> writer_;
  std::vector<type> types_;
};

/// Interprets a chunk as segment.
class segment_viewer {
public:
  /// Constructs a segment viewer from a chunk.
  /// @param chk The chunk representing a segment.
  explicit segment_viewer(chunk_ptr chk);

  /// Retrieves an event having a particular ID.
  /// @param id The event ID of the event to retrieve.
  /// @returns The event having ID *id*.
  expected<event> get(event_id id) const;

  /// Retrieves a set of events according to a bitmap.
  /// @param ids The event IDs of the events to retrieve.
  /// @returns The events according to *ids*.
  expected<std::vector<event>> get(const bitmap& ids) const;

  /// @returns The number of events in the segment.
  size_t size() const;

  /// @returns The ID of the first event.
  event_id base() const;

private:
  expected<event> extract(size_t offset) const;

  layout::viewer viewer_;
  std::vector<type> types_;
};

} // namespace vast

#endif
