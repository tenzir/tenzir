#include "vast/segment.h"

#include "vast/event.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/util/make_unique.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

// FIXME: Why does the linker complain without these definitions? These are
// redundant to those in the header file.
uint32_t const segment::magic;
uint8_t const segment::version;

segment_header::segment_header()
{
  start = now();
  end = start;
}

bool operator==(segment_header const& x, segment_header const& y)
{
  return x.id == y.id
      && x.base == y.base
      && x.compression == y.compression
      && x.start == y.start
      && x.end == y.end
      && x.n == y.n;
}

void segment_header::serialize(serializer& sink) const
{
  sink << segment::magic;
  sink << segment::version;
  sink << id;
  sink << compression;
  sink << start;
  sink << end;
  sink << base;
  sink << n;
}

void segment_header::deserialize(deserializer& source)
{
  uint32_t magic;
  source >> magic;
  if (magic != segment::magic)
    throw error::segment("invalid segment magic");

  uint8_t version;
  source >> version;
  if (version > segment::version)
    throw error::segment("segment version too high");

  source >> id;
  source >> compression;
  source >> start;
  source >> end;
  source >> base;
  source >> n;
}

segment::writer::writer(segment* s)
  : segment_(s),
    putter_(&chunk_),
    start_(now()),
    end_(start_)
{
}

void segment::writer::operator<<(event const& e)
{
  VAST_ENTER(VAST_ARG(e));
  if (e.timestamp() < start_)
    start_ = e.timestamp();
  if (e.timestamp() > end_)
    end_ = e.timestamp();
  ++n_;
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
    auto& hdr = segment_->header_;
    if (start_ < hdr.start)
      hdr.start = start_;
    if (end_ > hdr.end)
      hdr.end = end_;
    hdr.n += n_;

    segment_->chunks_.emplace_back(std::move(chunk_));
    chunk_bytes_ += chunk_.bytes();

    chunk_ = chunk_type();
    start_ = now();
    end_ = start_;
    n_ = 0;
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

segment_header& segment::header()
{
  return header_;
}

segment_header const& segment::header() const
{
  return header_;
}

uint32_t segment::events() const
{
  return header_.n;
}

size_t segment::size() const
{
  return chunks_.size();
}

uuid const& segment::id() const
{
  return header_.id;
}

void segment::serialize(serializer& sink) const
{
  sink << header_;
  sink.begin_sequence(chunks_.size());
  for (auto& tuple : chunks_)
    sink << cppa::get<0>(tuple);
  sink.end_sequence();
}

void segment::deserialize(deserializer& source)
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
  // TODO: maybe don't compare the full vector.
  return x.header_ == y.header_ && x.chunks_ == y.chunks_;
}

} // namespace vast
