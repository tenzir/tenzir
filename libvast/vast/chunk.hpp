#ifndef VAST_CHUNK_HPP
#define VAST_CHUNK_HPP

#include <unordered_map>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/message.hpp>
#include <caf/streambuf.hpp>

#include "vast/aliases.hpp"
#include "vast/bitstream.hpp"
#include "vast/compression.hpp"
#include "vast/time.hpp"
#include "vast/maybe.hpp"
#include "vast/schema.hpp"
#include "vast/streambuf.hpp"
#include "vast/util/operators.hpp"

namespace vast {

class event;

/// A compressed seqeuence of events. The events in the chunk must either all
/// have invalid IDs, i.e., equal to 0, or monotonically increasing IDs.
class chunk : util::equality_comparable<chunk> {
  friend access;

public:
  /// A proxy class to write events into the chunk. Upon descruction, the
  /// writer flushes its lingering state into the chunk.
  class writer {
  public:
    /// Constructs a writer from a chunk.
    /// @param chk The chunk to serialize into.
    writer(chunk& chk);

    writer(writer&&) = default;
    writer& operator=(writer&&) = default;

    /// Writes an event into the chunk.
    /// @param e The event to serialize.
    bool write(event const& e);

  private:
    chunk& chunk_;
    std::unordered_map<type, uint32_t> type_cache_;
    // TODO: consolidate to use single buffer.
    caf::vectorbuf vectorbuf_;
    compressedbuf compressedbuf_;
    caf::stream_serializer<compressedbuf&> serializer_;
  };

  /// A proxy class to read events from the chunk.
  class reader {
  public:
    /// Constructs a reader from a chunk.
    /// @param chk The chunk to extract objects from.
    reader(chunk const& chk);

    /// Extracts an event from the chunk.
    /// @param id Specifies the ID of the event to extract.
    /// @returns The extracted event, nothing if no more
    ///          events are available, or an error on failure.
    maybe<event> read(event_id id = invalid_event_id);

  private:
    maybe<event> materialize(bool discard);

    chunk const& chunk_;
    std::unordered_map<uint32_t, type> type_cache_;
    // TODO: consolidate to use single buffer.
    caf::charbuf charbuf_;
    compressedbuf compressedbuf_;
    caf::stream_deserializer<compressedbuf&> deserializer_;
    uint64_t available_ = 0;
    default_bitstream::const_iterator ids_begin_;
    default_bitstream::const_iterator ids_end_;
    event_id first_ = invalid_event_id;
  };

  /// Constructs a chunk.
  /// @param method The compression method to use.
  chunk(compression method = compression::null);

  /// Sets the mask of event IDs.
  /// @param ids The mask representing the IDs for the events in this chunk.
  /// @returns `true` if *ids* is a valid mask.
  bool ids(default_bitstream ids);

  /// Compresses a vector of events into this chunk.
  /// Destroys all previous contents.
  /// @param events The vector of events to write into this chunk.
  /// @returns `true` on success.
  maybe<void> compress(std::vector<event> const& events);

  /// Uncompresses the chunk back into a vector of events.
  /// @returns The vector of events for this chunk.
  std::vector<event> uncompress() const;

  /// Retrieves the number of events in the chunk.
  /// @returns The number of events in the chunk.
  uint64_t events() const;

  /// Retrieves the ID of the first event.
  /// @returns The ID of the first event.
  event_id base() const;

  friend bool operator==(chunk const& x, chunk const& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, chunk& chk) {
    return f(chk.events_,
             chk.first_,
             chk.last_,
             chk.ids_,
             chk.schema_,
             chk.compression_method_,
             chk.buffer_);
  }

private:
  uint64_t events_ = 0;
  time::point first_ = time::duration{};
  time::point last_ = time::duration{};
  default_bitstream ids_;
  vast::schema schema_;
  compression compression_method_;
  std::vector<char> buffer_;
};

} // namespace vast

#endif
