#include "vast/segment.h"

#include <ze/event.h>
#include <ze/logger.h>
#include <ze/io/serialization.h>
#include <ze/util/make_unique.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

// FIXME: Why does the linker complain without these definitions? These are
// redundant to those in the header file.
uint32_t const segment::magic;
uint8_t const segment::version;

segment::header::event_meta_data::event_meta_data()
{
  start = ze::now();
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

void segment::header::event_meta_data::accommodate(ze::event const& event)
{
  if (event.timestamp() < start)
    start = event.timestamp();
  if (event.timestamp() > end)
    end = event.timestamp();
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

void segment::header::serialize(ze::io::serializer& sink)
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

void segment::header::deserialize(ze::io::deserializer& source)
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

void segment::writer::operator<<(ze::event const& event)
{
  ZE_ENTER(ZE_ARG(event));
  event_meta_.accommodate(event);
  putter_ << event;
}

void segment::writer::flush()
{
  ZE_ENTER();
  processed_bytes_ += putter_.bytes();
  putter_.reset(); // Flushes and releases reference to chunk_.
  auto not_empty = ! chunk_.empty();
  if (not_empty)
  {
    segment_->header_.event_meta += event_meta_;
    segment_->chunks_.emplace_back(std::move(chunk_));
    chunk_bytes_ += chunk_.bytes();
    chunk_ = chunk();
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

void segment::reader::operator>>(ze::event& e)
{
  ZE_ENTER();
  ZE_MSG("available events: " << available_events());
  if (available_events() == 0)
  {
    if (chunk_ == segment_->chunks_.end())
      throw error::segment("no more events available");

    processed_bytes_ += getter_.bytes();
    chunk_bytes_ += cppa::get<0>(*chunk_).bytes();
    getter_.reset(&cppa::get<0>(*chunk_++));
  }

  getter_ >> e;
  ZE_LEAVE("got event: " << e);
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


segment::segment(ze::uuid uuid, ze::io::compression method)
{
  header_.id = std::move(uuid);
  header_.version = version;
  header_.compression = method;
}

segment::segment(segment const& other)
  : header_(other.header_),
    chunks_(other.chunks_)
{
  ZE_WARN("copied a segment!");
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

  // FIXME: do not hardcode size of ze::serialization.
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

ze::uuid const& segment::id() const
{
  return header_.id;
}

void segment::serialize(ze::io::serializer& sink)
{
  sink << header_;
  sink.write_sequence_begin(chunks_.size());
  for (auto& tuple : chunks_)
    sink << cppa::get<0>(tuple);
}

void segment::deserialize(ze::io::deserializer& source)
{
  source >> header_;
  uint64_t n;
  source.read_sequence_begin(n);
  chunks_.resize(n);
  for (auto& tuple : chunks_)
  {
    chunk chk;
    source >> chk;
    tuple = std::move(cppa::make_cow_tuple(std::move(chk)));
  }
}

bool operator==(segment const& x, segment const& y)
{
  return x.header_ == y.header_ && x.chunks_ == y.chunks_;
}

} // namespace vast
