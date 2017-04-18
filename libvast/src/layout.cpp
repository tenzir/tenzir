#include <numeric>

#include "vast/layout.hpp"

#include "vast/detail/varbyte.hpp"

namespace vast {

layout::writer::writer(std::streambuf& streambuf)
  : streambuf_{streambuf},
    serializer_{streambuf_},
    deserializer_{streambuf_} {
}

layout::writer::~writer() {
  finish();
}

size_t layout::writer::finish() {
  if (offsets_.empty())
    return 0;
  // Delta-encode offsets and serialize them.
  std::adjacent_difference(offsets_.begin(), offsets_.end(), offsets_.begin());
  entry_type offsets_position = streambuf_.put();
  serializer_ << offsets_;
  // Write offset position as trailing bytes.
  char buf[sizeof(entry_type)];
  auto ptr = reinterpret_cast<entry_type*>(&buf);
  *ptr = detail::to_network_order(offsets_position);
  serializer_.apply_raw(sizeof(buf), buf);
  // Enable re-use of writer by resetting offset table.
  offsets_.clear();
  return streambuf_.put();
}

size_t layout::writer::size() const {
  return offsets_.size();
}

uint64_t bytes(const layout::writer& p) {
  return p.streambuf_.put();
}

layout::reader::reader(std::streambuf& streambuf)
  : streambuf_{streambuf},
    deserializer_{streambuf_} {
  // Locate offset table position.
  auto pos = streambuf.pubseekoff(-4, std::ios::end, std::ios::in);
  if (pos == -1)
    return;
  char buf[4];
  auto got = streambuf.sgetn(buf, 4);
  if (got != 4)
    return;
  auto ptr = reinterpret_cast<entry_type*>(buf);
  auto offset_table = detail::to_host_order(*ptr);
  // Read offsets.
  pos = streambuf.pubseekoff(offset_table, std::ios::beg, std::ios::in);
  if (pos == -1)
    return;
  deserializer_ >> offsets_;
  VAST_ASSERT(!offsets_.empty());
  // Delta-decode offsets.
  std::partial_sum(offsets_.begin(), offsets_.end(), offsets_.begin());
}

size_t layout::reader::size() const {
  return offsets_.size();
}

layout::viewer::viewer(chunk_ptr chk)
  : charbuf_{*chk},
    deserializer_{charbuf_},
    offsets_{offset_table_start(chk)},
    chunk_{chk} {
  VAST_ASSERT(chk != nullptr);
}

layout::viewer::viewer(const viewer& other)
  : charbuf_{*other.chunk_},
    deserializer_{charbuf_},
    offsets_{offset_table_start(other.chunk_)},
    chunk_{other.chunk_} {
}

const char* layout::viewer::operator[](size_t i) const {
  VAST_ASSERT(chunk_);
  return chunk_->data() + offsets_[i];
}

const char* layout::viewer::at(size_t i) const {
  return !chunk_ || i >= chunk_->size()
    ? nullptr
    : chunk_->data() + offsets_[i];
}

size_t layout::viewer::size() const {
  return offsets_.size();
}

chunk_ptr layout::viewer::chunk() const {
  return chunk_;
}

layout::viewer::offset_table::offset_table(const char* ptr) : table_{ptr} {
  table_ += detail::varbyte::decode(size_, table_);
}

size_t layout::viewer::offset_table::operator[](size_t i) const {
  VAST_ASSERT(i < size());
  // On-the-fly partial sum with delta decoding.
  auto result = size_t{0};
  auto ptr = table_;
  for (auto j = 0u; j <= i; ++j) {
    size_t delta;
    ptr += detail::varbyte::decode(delta, ptr);
    result += delta;
  }
  return result;
}

size_t layout::viewer::offset_table::size() const {
  return size_;
}

const char* layout::viewer::offset_table_start(chunk_ptr chk) {
  auto ptr = chk->end() - sizeof(entry_type);
  auto off = detail::to_host_order(*reinterpret_cast<const entry_type*>(ptr));
  return chk->data() + off;
}

} // namespace vast
