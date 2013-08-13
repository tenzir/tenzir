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

  if (writer_)
    processed_bytes_ = writer_->bytes();

  writer_.reset();

  if (full())
    return false;

  segment_->occupied_bytes_ += chunk_->bytes();
  segment_->processed_bytes_ += processed_bytes_;
  segment_->n_ += chunk_->size();
  segment_->chunks_.emplace_back(std::move(*chunk_));

  chunk_ = make_unique<chunk>(segment_->compression_);
  writer_ = make_unique<chunk::writer>(*chunk_);
  processed_bytes_ = 0;

  return true;
}

bool segment::writer::full() const
{
  return max_segment_size_ > 0
      && segment_->occupied_bytes_ + chunk_->bytes() > max_segment_size_;
}

size_t segment::writer::bytes() const
{
  return writer_ ? writer_->bytes() : processed_bytes_;
}


segment::reader::reader(segment const* s)
  : segment_(s)
{
  if (! segment_->chunks_.empty())
    reader_ = make_unique<chunk::reader>(
        cppa::get<0>(segment_->chunks_[next_++]));
}

bool segment::reader::read(event& e)
{
  if (! reader_)
    return false;

  if (reader_->size() == 0)
  {
    if (next_ == segment_->chunks_.size()) // no more events available
      return false;

    processed_bytes_ += reader_->bytes();
    auto& next_chunk = cppa::get<0>(segment_->chunks_[next_++]);
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

size_t segment::reader::bytes() const
{
  return processed_bytes_ + (reader_ ? reader_->bytes() : 0);
}

segment::segment(uuid id, io::compression method)
  : id_(id),
    compression_(method)
{
}

segment::chunk_tuple segment::operator[](size_t i) const
{
  assert(! chunks_.empty());
  assert(i < chunks_.size());
  return chunks_[i];
}

uint32_t segment::events() const
{
  return n_;
}

size_t segment::size() const
{
  return chunks_.size();
}

uuid const& segment::id() const
{
  return id_;
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
  sink << processed_bytes_;
  sink << occupied_bytes_;

  sink.begin_sequence(chunks_.size());
  for (auto& tuple : chunks_)
    sink << cppa::get<0>(tuple);
  sink.end_sequence();
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
  source >> processed_bytes_;
  source >> occupied_bytes_;

  uint64_t n;
  source.begin_sequence(n);
  chunks_.resize(n);
  for (auto& tuple : chunks_)
  {
    chunk chk;
    source >> chk;
    tuple = std::move(cppa::make_cow_tuple(std::move(chk)));
  }
  source.end_sequence();
}

bool operator==(segment const& x, segment const& y)
{
  return x.id_ == y.id_;
}

} // namespace vast
