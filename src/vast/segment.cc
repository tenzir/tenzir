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
    writer_->write(e.timestamp(), 0);
    writer_->write(static_cast<std::vector<value> const&>(e));

  if (! success)
    VAST_LOG_ERROR("failed to write event entirely to chunk");

  return success;
}


segment::reader::reader(segment const* s)
  : segment_(s),
    current_id_(segment_->base_)
{
  if (! segment_->chunks_.empty())
    reader_ = make_unique<chunk::reader>(*segment_->chunks_[next_++]);
}

bool segment::reader::read(event& e)
{
  return read(&e);
}

bool segment::reader::skip_to(uint64_t id)
{
  if (! reader_)
    return false;

  if (segment_->base_ == 0)
    return false;

  if (id < current_id_ || id > segment_->base_ + segment_->n_)
    return false;

  auto elements = reader_->elements();
  chunk const* chk;
  do
  {
    if (current_id_ + elements < id)
    {
      current_id_ += elements;
      chk = &segment_->chunks_[next_].read();
      elements = chk->elements();
      reader_.reset();
      continue;
    }

    if (! reader_)
      reader_ = make_unique<chunk::reader>(*chk);

    while (current_id_ < id)
      if (! read(nullptr))
        return false;
    return true;
  }
  while (next_++ < segment_->chunks_.size());

  return false;
}

bool segment::reader::empty() const
{
  return reader_ ? reader_->elements() == 0 : true;
}

bool segment::reader::read(event* e)
{
  if (! reader_)
    return false;

  if (reader_->elements() == 0)
  {
    if (next_ == segment_->chunks_.size()) // No more events available.
      return false;

    auto& next_chunk = *segment_->chunks_[next_++];
    reader_ = make_unique<chunk::reader>(next_chunk);
    return read(e);
  }

  return load(e);
}

bool segment::reader::load(event* e)
{
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
    if (current_id_ > 0)
      r.id(current_id_);
    *e = std::move(r);
  }

  if (current_id_ > 0)
    ++current_id_;
  return true;
}


segment::segment(uuid id, size_t max_size, io::compression method)
  : id_(id),
    compression_(method),
    max_size_(max_size)
{
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

uint32_t segment::events() const
{
  return n_;
}

uint32_t segment::bytes() const
{
  return occupied_bytes_;
}

size_t segment::max_size() const
{
  return max_size_;
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

optional<event> segment::load(uint64_t id) const
{
  reader r(this);
  if (! r.skip_to(id))
    return {};

  event e;
  if (! r.read(e))
    return {};

  return {std::move(e)};
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

bool segment::append(chunk& c)
{
  if (max_size_ > 0 && bytes() + c.compressed_bytes() > max_size_)
    return false;

  n_ += c.elements();
  occupied_bytes_ += c.compressed_bytes();
  chunks_.emplace_back(std::move(c));
  return true;
}

} // namespace vast
