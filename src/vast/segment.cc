#include "vast/segment.h"

#include "vast/event.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/util/make_unique.h"

namespace vast {

// FIXME: Why does the linker complain without these definitions? These are
// redundant to those in the header file.
uint32_t const segment::magic;
uint8_t const segment::version;

bool operator==(segment const& x, segment const& y)
{
  return x.id_ == y.id_;
}

segment::writer::writer(segment* s, size_t max_events_per_chunk)
  : segment_(s),
    chunk_(make_unique<chunk>(segment_->compression_)),
    writer_(make_unique<chunk::writer>(*chunk_)),
    max_events_per_chunk_(max_events_per_chunk)
{
  assert(s != nullptr);
}

segment::writer::~writer()
{
  if (! flush())
    VAST_LOG_WARN("segment writer discarded " <<
                  chunk_->elements() << " events");
}

bool segment::writer::write(event const& e)
{
  if (! (writer_ && store(e)))
    return false;

  if (max_events_per_chunk_ > 0 &&
      chunk_->elements() % max_events_per_chunk_ == 0)
    flush();

  return true;
}

void segment::writer::attach_to(segment* s)
{
  assert(s != nullptr);
  segment_ = s;
}

bool segment::writer::flush()
{
  if (chunk_->empty())
    return true;

  writer_.reset();
  if (! segment_->append(*chunk_))
    return false;

  chunk_ = make_unique<chunk>(segment_->compression_);
  writer_ = make_unique<chunk::writer>(*chunk_);

  return true;
}

size_t segment::writer::bytes() const
{
  return writer_ ? writer_->bytes() : chunk_->uncompressed_bytes();
}

bool segment::writer::store(event const& e)
{
  auto success =
    writer_->write(e.name(), 0) &&
    writer_->write(e.timestamp(), 0) &&
    writer_->write(static_cast<std::vector<value> const&>(e));

  if (! success)
    VAST_LOG_ERROR("failed to write event entirely to chunk");

  return success;
}


segment::reader::reader(segment const* s)
  : segment_{*s},
    next_{segment_.base_},
    chunk_base_{segment_.base_}
{
  if (! segment_.chunks_.empty())
  {
    current_ = &segment_.chunks_.front().read();
    reader_ = make_unique<chunk::reader>(*current_);
  }
}

event_id segment::reader::position() const
{
  return next_;
}

optional<event> segment::reader::read(event_id id)
{
  if (id > 0 && ! seek(id))
    return {};

  event e;
  if (! load(&e))
    return {};

  return {std::move(e)};
}

bool segment::reader::seek(event_id id)
{
  if (! segment_.contains(id))
  {
    return false;
  }
  else if (id == next_)
  {
    return true;
  }
  else if (id < next_)
  {
    if (within_current_chunk(id))
      backup();
    else
      while (next_ > id)
        if (! prev())
          return false;
  }
  else
  {
    while (! within_current_chunk(id))
      if (! next())
        return false;
  }

  assert(id >= next_);
  auto n = id - next_;
  return skip(n) == n;
}

optional<size_t> segment::reader::extract(event_id begin,
                                          event_id end,
                                          std::function<void(event)> f)
{
  if (! segment_.contains(next_) ||
      ! f ||
      (begin > 0 && begin < segment_.base() && ! seek(begin)) ||
      (end > 0 && end >= segment_.base() + segment_.events()))
    return {};

  optional<size_t> n = 0;
  event_id i = next_ - 1;
  do
  {
    auto e = read(++i);
    if (! e)
    {
      VAST_LOG_ERROR("failed to read event " << i << " from chunk");
      return {};
    }

    f(std::move(*e));
    ++*n;
  }
  while ((end == 0 && within_current_chunk(i)) || i < end);

  return n;
}

chunk const* segment::reader::next()
{
  if (! current_ || chunk_idx_ + 1 == segment_.chunks_.size())
    return nullptr;

  if (next_ > 0)
  {
    chunk_base_ += current_->elements();
    next_ = chunk_base_;
  }

  current_ = &segment_.chunks_[++chunk_idx_].read();
  reader_ = make_unique<chunk::reader>(*current_);

  return current_;
}

chunk const* segment::reader::prev()
{
  if (segment_.chunks_.empty() || chunk_idx_ == 0)
    return nullptr;

  current_ = &segment_.chunks_[--chunk_idx_].read();
  reader_ = make_unique<chunk::reader>(*current_);

  if (next_ > 0)
  {
    chunk_base_ -= current_->elements();
    next_ = chunk_base_;
  }

  return current_;
}

event_id segment::reader::backup()
{
  if (! current_ || next_ == chunk_base_ || ! within_current_chunk(next_))
    return 0;

  auto distance = next_ - chunk_base_;
  next_ = chunk_base_;
  reader_ = make_unique<chunk::reader>(*current_);
  return distance;
}

bool segment::reader::load(event* e)
{
  if (! reader_ || reader_->available() == 0)
    return next() ? load(e) : false;

  string name;
  if (! reader_->read(name, 0))
  {
    VAST_LOG_ERROR("failed to read event name from chunk");
    return false;
  }

  time_point t;
  if (! reader_->read(t, 0))
  {
    VAST_LOG_ERROR("failed to read event timestamp from chunk");
    return false;
  }

  std::vector<value> v;
  if (! reader_->read(v))
  {
    VAST_LOG_ERROR("failed to read event arguments from chunk");
    return false;
  }

  if (e != nullptr)
  {
    event r(std::move(v));
    r.name(std::move(name));
    r.timestamp(t);
    if (next_ > 0)
      r.id(next_);
    *e = std::move(r);
  }

  if (next_ > 0)
    ++next_;

  return true;
}

event_id segment::reader::skip(size_t n)
{
  if (n == 0)
    return 0;

  event_id skipped = 0;
  while (n --> 0)
  {
    if (! load(nullptr))
      break;
    ++skipped;
  }

  return skipped;
}

bool segment::reader::within_current_chunk(event_id eid) const
{
  assert(current_ != nullptr);
  return next_ > 0
      && eid >= chunk_base_
      && eid < chunk_base_ + current_->elements();
}


segment::segment(uuid id, size_t max_bytes, io::compression method)
  : id_(id),
    compression_(method),
    max_bytes_(max_bytes)
{
}

uuid const& segment::id() const
{
  return id_;
}

void segment::base(event_id id)
{
  base_ = id;
}

event_id segment::base() const
{
  return base_;
}

bool segment::contains(event_id eid) const
{
  return base_ != 0 && base_ <= eid && eid < base_ + n_;
}

bool segment::contains(event_id from, event_id to) const
{
  return base_ != 0 && from < to && base_ <= from && to < base_ + n_;
}

uint32_t segment::events() const
{
  return n_;
}

uint32_t segment::bytes() const
{
  return occupied_bytes_;
}

size_t segment::max_bytes() const
{
  return max_bytes_;
}

size_t segment::store(std::vector<event> const& v, size_t max_events_per_chunk)
{
  writer w(this, max_events_per_chunk);
  size_t i;
  for (i = 0; i < v.size(); ++i)
    if (! w.write(v[i]))
      break;
  return i;
}

optional<event> segment::load(event_id id) const
{
  return reader{this}.read(id);
}

bool segment::append(chunk& c)
{
  if (max_bytes_ > 0 && bytes() + c.compressed_bytes() > max_bytes_)
    return false;

  n_ += c.elements();
  occupied_bytes_ += c.compressed_bytes();
  chunks_.emplace_back(std::move(c));
  return true;
}

void segment::serialize(serializer& sink) const
{
  sink << magic;
  sink << version;

  sink << id_;
  sink << compression_;
  sink << base_;
  sink << n_;
  sink << occupied_bytes_;
  sink << chunks_;
}

void segment::deserialize(deserializer& source)
{
  uint32_t m;
  source >> m;
  if (m != magic)
    throw std::runtime_error("invalid segment magic");

  uint8_t v;
  source >> v;
  if (v > segment::version)
    throw std::runtime_error("segment version too high");

  source >> id_;
  source >> compression_;
  source >> base_;
  source >> n_;
  source >> occupied_bytes_;
  source >> chunks_;
}

} // namespace vast
