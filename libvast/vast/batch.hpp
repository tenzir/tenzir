#ifndef VAST_BATCH_HPP
#define VAST_BATCH_HPP

#include <cstdint>
#include <unordered_map>
#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/message.hpp>
#include <caf/streambuf.hpp>

#include "vast/aliases.hpp"
#include "vast/bitmap.hpp"
#include "vast/compression.hpp"
#include "vast/detail/compressedbuf.hpp"
#include "vast/expected.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

namespace vast {

class event;

/// A compressed sequence of events.
class batch {
  using buffer_type = std::vector<char>;
  using size_type = uint64_t;

public:
  /// A proxy class to write events into the batch.
  class writer;

  /// A proxy class to read events from the batch.
  class reader;

  /// Constructs an empty batch.
  batch() = default;

  /// Assigns event IDs to the batch.
  /// @param begin The ID of the first event in the batch.
  /// @param end The ID one past the last ID in the batch.
  /// @returns `true` if *[begin,end)* is a valid event ID sequence, i.e.,
  ///          `end - begin == events()`
  bool ids(event_id begin, event_id end);

  /// Assigns event IDs to the batch.
  /// @param bm The bitmap representing the IDs for the events in this batch.
  /// @returns `true` if *ids* is a valid bitmap, i.e., `rank(ids) == events()`.
  bool ids(bitmap bm);

  /// Retrieves the bitmap of IDs for this batch
  const bitmap& ids() const;

  /// Retrieves the number of events in the batch.
  /// @returns The number of events in the batch.
  size_type events() const;

  template <class Inspector>
  friend auto inspect(Inspector& f, batch& b) {
    return f(b.method_, b.first_, b.last_, b.events_, b.ids_, b.data_);
  }

private:
  compression method_;
  timestamp first_ = timestamp::max();
  timestamp last_ = timestamp::min();
  size_type events_ = 0;
  bitmap ids_;
  buffer_type data_;
};

class batch::writer {
public:
  /// Constructs a writer from a batch.
  /// @param method The compression method to use.
  writer(compression method = compression::null);

  /// Writes an event into the batch.
  /// @param e The event to serialize.
  bool write(event const& e);

  /// Constructs a batch from the accumulated events.
  batch seal();

private:
  batch batch_;
  std::unordered_map<type, uint32_t> type_cache_;
  caf::vectorbuf vectorbuf_;
  detail::compressedbuf compressedbuf_;
  caf::stream_serializer<detail::compressedbuf&> serializer_;
};

class batch::reader {
public:
  /// Constructs a reader from a batch.
  /// @param b The batch to extract objects from.
  reader(batch const& b);

  /// Extracts all events.
  /// @returns The set events in the corresponding batch.
  expected<std::vector<event>> read();

  /// Extracts events according to a bitmap.
  /// @param ids The set of event IDs encoded as bitmap.
  /// @returns The set events according to *ids*.
  expected<std::vector<event>> read(const bitmap& ids);

private:
  expected<event> materialize();

  buffer_type const& data_;
  std::unordered_map<uint32_t, type> type_cache_;
  select_range<bitmap_bit_range> id_range_;
  size_type available_;
  caf::charbuf charbuf_;
  detail::compressedbuf compressedbuf_;
  caf::stream_deserializer<detail::compressedbuf&> deserializer_;
};

} // namespace vast

#endif
