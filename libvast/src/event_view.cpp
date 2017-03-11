#include "vast/event_view.hpp"

#include "vast/event.hpp"

namespace vast {

flatbuffers::Offset<detail::Event>
build(flatbuffers::FlatBufferBuilder& builder, const event& x) {
  auto data = build(builder, x.data());
  detail::EventBuilder eb{builder};
  eb.add_data(data);
  eb.add_meta(0); // TODO
  return eb.Finish();
}

} // namespace vast

