#include <caf/streambuf.hpp>

#include "vast/event.hpp"
#include "vast/bitmap.hpp"
#include "vast/segment.hpp"

#include "vast/detail/assert.hpp"

namespace vast {
namespace {

struct meta_data {
  event_id id;
  timestamp ts;
};

struct event_pack_helper {
  meta_data meta;
  uint32_t type_index;
  const vast::data& data;
};

struct event_unpack_helper {
  meta_data meta;
  uint32_t type_index;
  vast::data data;
};

template <class Inspector>
auto inspect(Inspector& f, meta_data& meta) {
  return f(meta.id, meta.ts);
}

template <class Inspector>
auto inspect(Inspector& f, event_pack_helper& helper) {
  return f(helper.meta, helper.type_index, helper.data);
}

template <class Inspector>
auto inspect(Inspector& f, event_unpack_helper& helper) {
  return f(helper.meta, helper.type_index, helper.data);
}

} // namespace <anonymous>

// We structure the layout internally as follows:
//
//     +---........---+---....---+
//     |  event data  |  types   |
//     +---........---+---....---+
//          0 - n        n + 1

segment_builder::segment_builder(size_t size)
  : streambuf_{std::make_unique<detail::mmapbuf>(size)},
    writer_{std::make_unique<layout::writer>(*streambuf_)} {
}

segment_builder::segment_builder(const path& filename, size_t size)
  : streambuf_{std::make_unique<detail::mmapbuf>(filename, size)},
    writer_{std::make_unique<layout::writer>(*streambuf_)} {
}

expected<void> segment_builder::put(const event& e) {
  if (streambuf_->size() == 0)
    return ec::unspecified;
  auto helper = event_pack_helper{{e.id(), e.timestamp()}, 0, e.data()};
  auto i = std::find(types_.begin(), types_.end(), e.type());
  if (i == types_.end())
    types_.push_back(e.type());
  helper.type_index = std::distance(types_.begin(), i);
  return writer_->write(helper);
}

expected<event> segment_builder::get(event_id id) const {
  if (!writer_)
    return ec::unspecified;
  auto first = writer_->read<event_id>(0);
  if (!first)
    return ec::unspecified;
  return extract(id - *first);
}

expected<std::vector<event>> segment_builder::get(const bitmap& ids) const {
  if (!writer_)
    return ec::unspecified;
  auto first = writer_->read<event_id>(0);
  if (!first)
    return ec::unspecified;
  std::vector<event> xs;
  for (auto id : select(ids)) {
    auto x = extract(id - *first);
    if (x)
      xs.push_back(std::move(*x));
    return x.error();
  }
  return xs;
}

expected<chunk_ptr> segment_builder::finish() {
  if (!writer_ || writer_->size() == 0)
    return ec::unspecified;
  // Write types at position n + 1.
  auto result = writer_->write(types_);
  if (!result)
    return result.error();
  auto bytes_written = writer_->finish();
  if (bytes_written < streambuf_->size() && !streambuf_->resize(bytes_written))
    return ec::unspecified;
  types_.clear();
  return streambuf_->release();
}

uint64_t bytes(const segment_builder& b) {
  return bytes(*b.writer_);
}

expected<event> segment_builder::extract(size_t offset) const {
  VAST_ASSERT(offset < writer_->size());
  auto helper = writer_->read<event_unpack_helper>(offset);
  if (!helper)
    return ec::unspecified;
  VAST_ASSERT(helper->type_index < types_.size());
  event e{value{std::move(helper->data), types_[helper->type_index]}};
  e.id(helper->meta.id);
  e.timestamp(helper->meta.ts);
  return e;
}

segment_viewer::segment_viewer(chunk_ptr chk) : viewer_{std::move(chk)} {
  if (viewer_.size() < 2) // Need at least one event plus the type table.
    return;
  if (auto types = viewer_.unpack<std::vector<type>>(viewer_.size() - 1))
    types_ = std::move(*types);
}

expected<event> segment_viewer::get(event_id id) const {
  if (viewer_.size() == 0)
    return ec::unspecified;
  auto first = viewer_.unpack<event_id>(0);
  if (!first)
    return ec::unspecified;
  auto idx = id - *first;
  return extract(idx);
}

expected<std::vector<event>> segment_viewer::get(const bitmap& ids) const {
  if (viewer_.size() == 0)
    return ec::unspecified;
  auto first = viewer_.unpack<event_id>(0);
  if (!first)
    return ec::unspecified;
  std::vector<event> xs;
  for (auto id : select(ids)) {
    auto x = extract(id - *first);
    if (x)
      xs.push_back(std::move(*x));
    return x.error();
  }
  return xs;
}

size_t segment_viewer::size() const {
  return viewer_.size() == 0 ? 0 : viewer_.size() - 1; // - 1 for type vector
}

event_id segment_viewer::base() const {
  if (viewer_.size() == 0)
    return invalid_event_id;
  if (auto first = viewer_.unpack<event_id>(0))
    return *first;
  return invalid_event_id;
}

expected<event> segment_viewer::extract(size_t offset) const {
  VAST_ASSERT(offset < size());
  auto helper = viewer_.unpack<event_unpack_helper>(offset);
  if (!helper)
    return helper.error();
  if (helper->type_index >= types_.size())
    return ec::unspecified;
  event e{value{std::move(helper->data), types_[helper->type_index]}};
  e.id(helper->meta.id);
  e.timestamp(helper->meta.ts);
  return e;
}

} // namespace vast
