#include "vast/segment.h"

#include "vast/event.h"
#include "vast/logger.h"
#include "vast/io/serialization.h"
#include "vast/util/make_unique.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

// FIXME: Why does the linker complain without these definitions? These are
// redundant to those in the header file.
uint32_t const segment::magic;
uint8_t const segment::version;

segment::header::event_meta_data::event_meta_data()
{
  start = now();
  end = start;
}

segment::header::event_meta_data&
segment::header::event_meta_data::operator+=(event_meta_data const& other)
{
  if (other.start < start)
    start = other.start;
  if (other.end > end)
    end = other.end;
  n += other.n;
  return *this;
}

bool operator==(segment::header::event_meta_data const& x,
                segment::header::event_meta_data const& y)
{
  return x.start == y.start && x.end == y.end && x.n == y.n;
}

void segment::header::event_meta_data::accommodate(event const& e)
{
  if (e.timestamp() < start)
    start = e.timestamp();
  if (e.timestamp() > end)
    end = e.timestamp();
  ++n;
}

bool operator==(segment::header const& x, segment::header const& y)
{
  return x.version == y.version &&
    x.id == y.id &&
    x.base == y.base &&
    x.compression == y.compression &&
    x.event_meta == y.event_meta;
}

void segment::header::serialize(io::serializer& sink)
{
  sink << segment::magic;
  sink << version;
  sink << id;
  sink << base;
  sink << compression;
  sink << event_meta.start;
  sink << event_meta.end;
  sink << event_meta.n;
}

void segment::header::deserialize(io::deserializer& source)
{
  uint32_t magic;
  source >> magic;
  if (magic != segment::magic)
    throw error::segment("invalid segment magic");

  source >> version;
  if (version > segment::version)
    throw error::segment("segment version too high");

  source >> id;
  source >> base;
  source >> compression;
  source >> event_meta.start;
  source >> event_meta.end;
  source >> event_meta.n;
}

segment::writer::writer(segment* s)
  : segment_(s),
    putter_(&chunk_)
{
}

void segment::writer::operator<<(event const& e)
{
  VAST_ENTER(VAST_ARG(e));
  event_meta_.accommodate(e);
  putter_ << e;
}

bool segment::writer::flush()
{
  VAST_ENTER();
  processed_bytes_ += putter_.bytes();
  putter_.reset(); // Flushes and releases reference to chunk_.
  auto not_empty = ! chunk_.empty();
  if (not_empty)
  {
    segment_->header_.event_meta += event_meta_;
    segment_->chunks_.emplace_back(std::move(chunk_));
    chunk_bytes_ += chunk_.bytes();
    chunk_ = chunk_type();
    event_meta_ = header::event_meta_data();
  }
  putter_.reset(&chunk_);
  return not_empty;
}

size_t segment::writer::elements() const
{
  return chunk_.size();
}

size_t segment::writer::processed_bytes() const
{
  return processed_bytes_ + putter_.bytes();
}

size_t segment::writer::chunk_bytes() const
{
  return chunk_bytes_;
}


segment::reader::reader(segment const* s)
  : segment_(s),
    chunk_(segment_->chunks_.begin())
{
  if (segment_->chunks_.empty())
    return;

  getter_.reset(&cppa::get<0>(segment_->chunks_.at(0)));
  ++chunk_;
}

segment::reader::operator bool () const
{
  return available_events() > 0 || available_chunks() > 0;
}

void segment::reader::operator>>(event& e)
{
  VAST_ENTER();
  VAST_MSG("available events: " << available_events());
  if (available_events() == 0)
  {
    if (chunk_ == segment_->chunks_.end())
      throw error::segment("no more events available");

    processed_bytes_ += getter_.bytes();
    chunk_bytes_ += cppa::get<0>(*chunk_).bytes();
    getter_.reset(&cppa::get<0>(*chunk_++));
  }

  getter_ >> e;
  VAST_LEAVE("got event: " << e);
}

uint32_t segment::reader::available_events() const
{
  return getter_.available();
}

size_t segment::reader::available_chunks() const
{
  return std::distance(chunk_, segment_->chunks_.end());
}

size_t segment::reader::processed_bytes() const
{
  return processed_bytes_ + getter_.bytes();
}

size_t segment::reader::chunk_bytes() const
{
  return chunk_bytes_;
}


segment::segment(uuid id, io::compression method)
{
  header_.id = std::move(id);
  header_.version = version;
  header_.compression = method;
}

segment::segment(segment const& other)
  : header_(other.header_),
    chunks_(other.chunks_)
{
  VAST_LOG_WARN("copied a segment!");
}

segment& segment::operator=(segment other)
{
  using std::swap;
  swap(header_, other.header_);
  swap(chunks_, other.chunks_);
  return *this;
}

segment::chunk_tuple segment::operator[](size_t i) const
{
  assert(! chunks_.empty());
  assert(i < chunks_.size());
  return chunks_[i];
}

segment::header const& segment::head() const
{
  return header_;
}

uint32_t segment::events() const
{
  return header_.event_meta.n;
}

/*
size_t segment::bytes() const
{
  // FIXME: compute incrementally rather than ad-hoc.
  static size_t constexpr header =
    sizeof(header_.version) +
    sizeof(header_.compression) +
    sizeof(header_.start) +
    sizeof(header_.end) +
    sizeof(header_.events);

  // FIXME: do not hardcode size of serialization.
  size_t events = 8;
  for (auto& str : header_.event_names)
    events += 4 + str.size();

  size_t chunks = 8;
  for (auto& chk : chunks_)
    chunks += 4 + 8 + chk.size();

  return header + events + chunks;
}
*/

size_t segment::size() const
{
  return chunks_.size();
}

uuid const& segment::id() const
{
  return header_.id;
}

void segment::serialize(io::serializer& sink)
{
  sink << header_;
  sink.begin_sequence(chunks_.size());
  for (auto& tuple : chunks_)
    sink << cppa::get<0>(tuple);
  sink.end_sequence();
}

void segment::deserialize(io::deserializer& source)
{
  source >> header_;
  uint64_t n;
  source.begin_sequence(n);
  chunks_.resize(n);
  for (auto& tuple : chunks_)
  {
    chunk_type chk;
    source >> chk;
    tuple = std::move(cppa::make_cow_tuple(std::move(chk)));
  }
  source.end_sequence();
}

bool operator==(segment const& x, segment const& y)
{
  return x.header_ == y.header_ && x.chunks_ == y.chunks_;
}

} // namespace vast
