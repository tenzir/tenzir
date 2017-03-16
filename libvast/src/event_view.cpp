#include "vast/event_view.hpp"

#include "vast/event.hpp"

namespace vast {

flatbuffers::Offset<detail::Event>
build(flatbuffers::FlatBufferBuilder& builder, const event&) {
  detail::EventBuilder eb{builder};
  // TODO
  eb.add_value(0);
  eb.add_meta(0);
  return eb.Finish();
}

} // namespace vast

