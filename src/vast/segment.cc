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

bool operator==(segment const& x, segment const& y)
{
  return x.id_ == y.id_;
}

segment::writer::writer(segment* s,
                        size_t max_events_per_chunk,
                        size_t max_segment_size)
  : segment_(s),
    chunk_(make_unique<chunk>(segment_->compression_)),
    writer_(make_unique<chunk::writer>(*chunk_)),
    max_events_per_chunk_(max_events_per_chunk),
    max_segment_size_(max_segment_size)
{
  assert(s != nullptr);
}

segment::writer::~writer()
{
  if (! flush())
    VAST_LOG_WARN("segment writer discarded " << chunk_->size() << " events");
}

bool segment::writer::write(event const& e)
{
  if (! (writer_ && writer_->write(e)))
    return false;

  if (max_events_per_chunk_ > 0 && chunk_->size() % max_events_per_chunk_ == 0)
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
  if (full())
    return false;

  segment_->append(std::move(*chunk_));
  chunk_ = make_unique<chunk>(segment_->compression_);
  writer_ = make_unique<chunk::writer>(*chunk_);

  return true;
}

bool segment::writer::full() const
{
  return max_segment_size_ > 0
      && segment_->bytes() + chunk_->compressed_bytes() > max_segment_size_;
}

size_t segment::writer::bytes() const
{
  return writer_ ? writer_->bytes() : chunk_->uncompressed_bytes();
}


segment::reader::reader(segment const* s)
  : segment_(s)
{
  if (! segment_->chunks_.empty())
    reader_ = make_unique<chunk::reader>(cget(segment_->chunks_[next_++]));
}

bool segment::reader::read(event& e)
{
  if (! reader_)
    return false;

  if (reader_->size() == 0)
  {
    if (next_ == segment_->chunks_.size()) // no more events available
      return false;

    auto& next_chunk = cget(segment_->chunks_[next_++]);
    reader_ = make_unique<chunk::reader>(next_chunk);
    return read(e);
  }

  reader_->read(e);
  return true;
}

bool segment::reader::empty() const
{
  return reader_ ? reader_->size() == 0 : true;
}

segment::segment(uuid id, io::compression method)
  : id_(id),
    compression_(method)
{
}

uuid const& segment::id() const
{
  return id_;
}

uint32_t segment::events() const
{
  return n_;
}

uint32_t segment::bytes() const
{
  return occupied_bytes_;
}

void segment::base(uint64_t id)
{
  base_ = id;
}

uint64_t segment::base() const
{
  return base_;
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
    throw error::segment("invalid segment magic");

  uint8_t v;
  source >> v;
  if (v > segment::version)
    throw error::segment("segment version too high");

  source >> id_;
  source >> compression_;
  source >> base_;
  source >> n_;
  source >> occupied_bytes_;
  source >> chunks_;
}

void segment::append(chunk c)
{
  n_ += c.size();
  occupied_bytes_ += c.compressed_bytes();
  chunks_.emplace_back(std::move(c));
}

} // namespace vast
