#ifndef VAST_CHUNK_H
#define VAST_CHUNK_H

#include <caf/message.hpp>
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/block.h"
#include "vast/time.h"
#include "vast/result.h"
#include "vast/schema.h"

namespace vast {

class event;

/// A compressed seqeuence of events. The events in the chunk must either all
/// have invalid IDs, i.e., equal to 0, or monotonically increasing IDs.
class chunk : util::equality_comparable<chunk>
{
public:
  /// Chunk meta data.
  struct meta_data : util::equality_comparable<meta_data>
  {
    time_point first = time_duration{};
    time_point last = time_duration{};
    default_bitstream ids;
    vast::schema schema;

  private:
    friend access;
    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);

    friend bool operator==(meta_data const& x, meta_data const& y);
  };

  /// A proxy class to write events into the chunk.
  class writer
  {
  public:
    /// Constructs a writer from a chunk.
    /// @param chk The chunk to serialize into.
    writer(chunk& chk);

    /// Destructs a chunk.
    ~writer();

    writer(writer&&) = default;
    writer& operator=(writer&&) = default;

    /// Writes an event into the chunk.
    /// @param e The event to serialize.
    bool write(event const& e);

    /// Flushes the current writer state to the underlying chunk. Can only
    /// called once for each writer and will also be called by the destructor.
    void flush();

  private:
    meta_data* meta_;
    std::unique_ptr<block::writer> block_writer_;
  };

  /// A proxy class to read events from the chunk.
  class reader
  {
  public:
    /// Constructs a reader from a chunk.
    /// @param chk The chunk to extract objects from.
    reader(chunk const& chk);

    /// Extracts an event from the chunk.
    /// @param id Specifies the ID of the event to extract.
    /// @returns The extracted event, an empty result if there are no more
    ///          events available, or an error on failure.
    result<event> read(event_id id = invalid_event_id);

  private:
    result<event> materialize(bool discard);

    chunk const* chunk_;
    std::unique_ptr<block::reader> block_reader_;
    default_bitstream::const_iterator ids_begin_;
    default_bitstream::const_iterator ids_end_;
    event_id first_ = invalid_event_id;
  };

  /// Constructs a chunk.
  /// @param method The compression method to use.
  chunk(io::compression method = io::lz4);

  /// Sets the mask of event IDs.
  /// @param ids The mask representing the IDs for the events in this chunk.
  /// @returns `true` if *ids* is a valid mask.
  bool ids(default_bitstream ids);

  /// Constructs a chunk and writes events into it.
  /// @param es The events to write into the chunk.
  /// @param method The compression method of the underlying block.
  chunk(std::vector<event> const& es, io::compression method = io::lz4);

  /// Retrieves the chunk meta data.
  /// @returns The meta data of the chunk.
  meta_data const& meta() const;

  /// Retrieves the size of the compressed chunk in bytes.
  /// @returns The number of bytes the chunk takes up in memory.
  uint64_t bytes() const;

  /// Retrieves the number of events in the chunk.
  /// @returns The number of events in the chunk.
  uint64_t events() const;

private:
  meta_data& get_meta();
  vast::block& block();
  vast::block const& block() const;

  caf::message msg_;  // <meta_data, block>

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend bool operator==(chunk const& x, chunk const& y);
};

} // namespace vast

#endif
