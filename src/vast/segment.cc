#include "vast/segment.h"

#include "vast/event.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/util/make_unique.h"

namespace vast {

// FIXME: Why does the linker complain without these definitions? These are
// redundant to those in the header file.
uint32_t const segment::header::magic;
uint32_t const segment::header::version;

void segment::header::serialize(serializer& sink) const
{
  sink
    << magic << version
    << id
    << compression
    << first
    << last
    << base
    << n
    << max_bytes
    << occupied_bytes
    << schema;
}

void segment::header::deserialize(deserializer& source)
{
  uint32_t m;
  source >> m;
  if (m != magic)
    throw std::runtime_error{"invalid segment magic"};

  uint32_t v;
  source >> v;
  if (v > version)
    throw std::runtime_error{"segment version too high"};

  source
    >> id
    >> compression
    >> first
    >> last
    >> base
    >> n
    >> max_bytes
    >> occupied_bytes
    >> schema;
}

bool operator==(segment::header const& x, segment::header const& y)
{
  return x.id == y.id
      && x.compression == y.compression
      && x.first == y.first
      && x.last == y.last
      && x.base == y.base
      && x.n == y.n
      && x.max_bytes == y.max_bytes
      && x.occupied_bytes == y.occupied_bytes
      && x.schema == y.schema;
}

segment::writer::writer(segment* s, size_t max_events_per_chunk)
  : segment_(s),
    chunk_{make_unique<chunk>(segment_->header_.compression)},
    chunk_writer_{make_unique<chunk::writer>(*chunk_)},
    max_events_per_chunk_{max_events_per_chunk}
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
  if (! util::get<invalid_type>(e.type()->info()) && ! e.type()->name().empty())
    if (! schema_.find_type(e.type()->name()) && ! schema_.add(e.type()))
      return false;

  if (! (chunk_writer_ && store(e)))
    return false;

  if (max_events_per_chunk_ && chunk_->elements() % max_events_per_chunk_ == 0)
    return !!flush();

  return true;
}

void segment::writer::attach_to(segment* s)
{
  assert(s != nullptr);
  segment_ = s;
}

trial<void> segment::writer::flush()
{
  if (chunk_->empty())
    return nothing;

  chunk_writer_.reset();

  if (segment_->max_bytes() > 0
      && segment_->bytes() + chunk_->compressed_bytes() > segment_->max_bytes())
    return error{"flushing " + to_string(chunk_->compressed_bytes()) +
                 "B would exceed segment capacity " +
                 to_string(segment_->max_bytes())};

  segment_->header_.first = first_;
  segment_->header_.last = last_;
  segment_->header_.n += chunk_->elements();
  segment_->header_.occupied_bytes += chunk_->compressed_bytes();
  segment_->chunks_.push_back(std::move(*chunk_));

  chunk_ = make_unique<chunk>(segment_->header_.compression);
  chunk_writer_ = make_unique<chunk::writer>(*chunk_);

  auto s = schema::merge(schema_, segment_->header_.schema);
  if (s)
    segment_->header_.schema = std::move(*s);
  else
    return s.error();


  first_ = time_range{};
  last_ = time_range{};

  return nothing;
}

size_t segment::writer::bytes() const
{
  return chunk_writer_ ? chunk_writer_->bytes() : chunk_->uncompressed_bytes();
}

bool segment::writer::store(event const& e)
{
  auto success =
    chunk_writer_->write(e.name(), 0) &&
    chunk_writer_->write(e.timestamp(), 0) &&
    chunk_writer_->write(static_cast<std::vector<value> const&>(e));

  if (first_ == time_range{} || e.timestamp() < first_)
    first_ = e.timestamp();

  if (last_ == time_range{} || e.timestamp() > last_)
    last_ = e.timestamp();

  if (! success)
    VAST_LOG_ERROR("failed to write event to chunk");

  return success;
}


segment::reader::reader(segment const* s)
  : segment_{*s},
    next_{segment_.header_.base},
    chunk_base_{segment_.header_.base}
{
  if (! segment_.chunks_.empty())
  {
    current_ = &segment_.chunks_.front().read();
    chunk_reader_ = make_unique<chunk::reader>(*current_);
  }
}

event_id segment::reader::position() const
{
  return next_;
}

trial<event> segment::reader::read(event_id id)
{
  if (id > 0 && ! seek(id))
    return error{"event id " + to_string(id) + " out of bounds"};

  auto r = load();
  if (r)
    return {std::move(r.value())};
  else if (r.failed())
    return r.error();

  assert(! r.empty());
  return error{"empty event"}; // should never happen.
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

  auto r = skip(n);
  if (r)
    return *r == n;

  VAST_LOG_ERROR(r.error().msg());
  return false;
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
  chunk_reader_ = make_unique<chunk::reader>(*current_);

  return current_;
}

chunk const* segment::reader::prev()
{
  if (segment_.chunks_.empty() || chunk_idx_ == 0)
    return nullptr;

  current_ = &segment_.chunks_[--chunk_idx_].read();
  chunk_reader_ = make_unique<chunk::reader>(*current_);

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
  chunk_reader_ = make_unique<chunk::reader>(*current_);
  return distance;
}

result<event> segment::reader::load(bool discard)
{
  if (! chunk_reader_ || chunk_reader_->available() == 0)
    return next() ? load(discard) : error{"no more events to load"};

  string name;
  if (! chunk_reader_->read(name, 0))
    return error{"failed to read type name from chunk"};

  time_point ts;
  if (! chunk_reader_->read(ts, 0))
    return error{"failed to read event timestamp from chunk"};

  std::vector<value> v;
  if (! chunk_reader_->read(v))
    return error{"failed to read event arguments from chunk"};

  if (! discard)
  {
    event e(std::move(v));
    e.timestamp(ts);
    if (next_ > 0)
      e.id(next_++);

    if (auto t = segment_.header_.schema.find_type(name))
      e.type(t);
    else if (! name.empty())
      VAST_LOG_WARN("schema inconsistency, missing type: " << name);

    return std::move(e);
  }

  if (next_ > 0)
    ++next_;

  return {};
}

trial<event_id> segment::reader::skip(size_t n)
{
  if (n == 0)
    return 0;

  event_id skipped = 0;
  while (n --> 0)
  {
    auto r = load(true);
    assert(! r);
    if (r.failed())
      return r.error();

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


segment::segment(uuid id, uint64_t max_bytes, io::compression method)
{
  header_.id = id;
  header_.compression = method;
  header_.max_bytes = max_bytes;
}

void segment::base(event_id id)
{
  header_.base = id;
}

trial<event> segment::load(event_id id) const
{
  return reader{this}.read(id);
}

uuid const& segment::id() const
{
  return header_.id;
}

time_point segment::first() const
{
  return header_.first;
}

time_point segment::last() const
{
  return header_.last;
}

event_id segment::base() const
{
  return header_.base;
}

bool segment::contains(event_id eid) const
{
  return header_.base != 0
      && header_.base <= eid
      && eid < header_.base + header_.n;
}

bool segment::contains(event_id from, event_id to) const
{
  return header_.base != 0
      && from < to && header_.base <= from
      && to < header_.base + header_.n;
}

uint64_t segment::events() const
{
  return header_.n;
}

uint64_t segment::bytes() const
{
  return header_.occupied_bytes;
}

uint64_t segment::max_bytes() const
{
  return header_.max_bytes;
}

schema const& segment::schema() const
{
  return header_.schema;
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

void segment::serialize(serializer& sink) const
{
  sink << header_ << chunks_;
}

void segment::deserialize(deserializer& source)
{
  source >> header_ >> chunks_;
}

bool operator==(segment const& x, segment const& y)
{
  return x.header_ == y.header_;
}

} // namespace vast
