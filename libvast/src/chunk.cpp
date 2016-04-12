#include "vast/caf.hpp"
#include "vast/chunk.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/concept/serializable/state.hpp"
#include "vast/concept/serializable/std/chrono.hpp"
#include "vast/concept/serializable/vast/data.hpp"
#include "vast/concept/serializable/vast/type.hpp"
#include "vast/concept/state/event.hpp"
#include "vast/util/assert.hpp"

namespace vast {

static_assert(!caf::detail::has_serialize<port::port_type>::value,
              "shouldn't have serialize!");

chunk::writer::writer(chunk& chk)
  : chunk_{chk},
    vectorbuf_{chunk_.buffer_},
    compressedbuf_{vectorbuf_, chunk_.compression_method_},
    serializer_{compressedbuf_} {
}

bool chunk::writer::write(event const& e) {
  // Write meta data.
  if (chunk_.first_ == time::duration{} || e.timestamp() < chunk_.first_)
    chunk_.first_ = e.timestamp();
  if (chunk_.last_ == time::duration{} || e.timestamp() > chunk_.last_)
    chunk_.last_ = e.timestamp();
  if (e.id() != invalid_event_id || !chunk_.ids_.empty()) {
    if (e.id() < chunk_.ids_.size() || e.id() == invalid_event_id)
      return false;
    auto delta = e.id() - chunk_.ids_.size();
    chunk_.ids_.append(delta, false);
    chunk_.ids_.push_back(true);
  }
  // Write type.
  auto t = type_cache_.find(e.type());
  if (t == type_cache_.end()) {
    if (!chunk_.schema_.add(e.type()))
      return false;
    auto type_id = static_cast<uint32_t>(type_cache_.size());
    t = type_cache_.emplace(e.type(), type_id).first;
    serializer_ << type_id << e.type().name();
  } else {
    serializer_ << t->second;
  }
  serializer_ << e.timestamp() << e.data();
  ++chunk_.events_;
  return true;
}

chunk::reader::reader(chunk const& chk)
  : chunk_{chk},
    charbuf_{const_cast<char*>(chunk_.buffer_.data()), chunk_.buffer_.size()},
    compressedbuf_{charbuf_},
    deserializer_{compressedbuf_},
    available_{chunk_.events_},
    ids_begin_{chunk_.ids_.begin()},
    ids_end_{chunk_.ids_.end()} {
  if (ids_begin_ != ids_end_)
    first_ = *ids_begin_;
}

maybe<event> chunk::reader::read(event_id id) {
  if (id != invalid_event_id) {
    if (first_ == invalid_event_id)
      return fail("chunk has no associated ids, cannot read event ", id);
    if (id < first_)
      return fail("chunk begins at id ", first_);
    if (ids_begin_ == ids_end_ || id < *ids_begin_)
      return fail("cannot seek back to earlier event");
    while (ids_begin_ != ids_end_ && *ids_begin_ < id) {
      auto e = materialize(true);
      if (!e)
        return e;
      ++ids_begin_;
    }
    if (ids_begin_ == ids_end_ || *ids_begin_ != id)
      return fail("no event with id ", id);
  }
  auto e = materialize(false);
  if (e && ids_begin_ != ids_end_)
    e->id(*ids_begin_++);
  return e;
}

maybe<event> chunk::reader::materialize(bool discard) {
  if (available_ == 0)
    return {};
  // Read type.
  uint32_t type_id;
  deserializer_ >> type_id;
  auto t = type_cache_.find(type_id);
  if (t == type_cache_.end()) {
    std::string type_name;
    deserializer_ >> type_name;
    auto st = chunk_.schema_.find(type_name);
    if (!st)
      return fail("schema inconsistency, missing type: ", type_name);
    t = type_cache_.emplace(type_id, *st).first;
  }
  // Read timstamp and data
  time::point ts;
  data d;
  deserializer_ >> ts >> d;
  // Bail out early if requested.
  if (discard)
    return {};
  event e{{std::move(d), t->second}};
  e.timestamp(ts);
  return std::move(e);
}

chunk::chunk(compression method)
  : compression_method_{method} {
}

bool operator==(chunk const& x, chunk const& y) {
  return x.events_ == y.events_
      && x.first_ == y.first_
      && x.last_ == y.last_
      && x.ids_ == y.ids_
      && x.schema_ == y.schema_
      && x.compression_method_ == y.compression_method_
      && x.buffer_ == y.buffer_;
}

bool chunk::ids(default_bitstream ids) {
  if (ids_.count() != events_)
    return false;
  ids_ = std::move(ids);
  return true;
}

maybe<void> chunk::compress(std::vector<event> const& events) {
  writer w{*this};
  for (auto& e : events) {
    auto m = w.write(e);
    if (!m)
      return fail();
  }
  return {};
}

std::vector<event> chunk::uncompress() const {
  std::vector<event> result(events_);
  reader r{*this};
  for (uint64_t i = 0; i < events_; ++i) {
    auto e = r.read();
    VAST_ASSERT(e);
    result[i] = std::move(*e);
  }
  return result;
}

uint64_t chunk::events() const {
  return events_;
}

event_id chunk::base() const {
  auto i = ids_.find_first();
  return i == default_bitstream::npos ? invalid_event_id : i;
}

} // namespace vast
