#ifndef VAST_EVENT_VIEW_HPP
#define VAST_EVENT_VIEW_HPP

#include "vast/data_view.hpp"

#include "vast/detail/event_generated.h"

namespace vast {

class event;

/// A view of an ::event.
class event_view {
public:
  event_view() = default;

  /// Constructs an event view from a chunk.
  /// @param chk The chunk to construct an event from
  explicit event_view(chunk_ptr chk);

  data_view data() const;

  event_id id() const;

private:
  const detail::Event* event_ = nullptr;
  chunk_ptr chunk_;
};

flatbuffers::Offset<detail::Event>
build(flatbuffers::FlatBufferBuilder& builder, const event& x);

} // namespace vast

#endif

