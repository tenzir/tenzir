#include "vast/segment.h"

#include <ze/event.h>
#include <ze/util/make_unique.h>
#include "vast/logger.h"

namespace vast {

// FIXME: Why does the linker complain without these definitions? These are
// redundant to those in the header file.
uint32_t const segment::magic;
uint8_t const segment::version;

segment::writer::writer(segment* s)
  : segment_(s)
  , putter_(chunk::putter(&chunk_))
{
}

segment::writer::writer(writer&& other)
  : bytes_(other.bytes_)
  , segment_(other.segment_)
  , chunk_(std::move(other.chunk_))
  , putter_(std::move(other.putter_))
{
  other.bytes_ = 0;
  other.segment_ = nullptr;
}

size_t segment::writer::bytes() const
{
  return bytes_;
}

size_t segment::writer::elements() const
{
  return chunk_.elements();
}

uint32_t segment::writer::operator<<(ze::event const& event)
{
  ++segment_->header_.events;

  if (event.timestamp() < segment_->header_.start)
    segment_->header_.start = event.timestamp();
  if (event.timestamp() > segment_->header_.end)
    segment_->header_.end = event.timestamp();

  auto i = std::lower_bound(segment_->header_.event_names.begin(),
                            segment_->header_.event_names.end(),
                            event.name());

  if (i == segment_->header_.event_names.end())
    segment_->header_.event_names.emplace_back(event.name().data());
  else if (event.name() < *i)
    segment_->header_.event_names.emplace(i, event.name().data());

  bytes_ = putter_ << event;

  return chunk_.elements();
}

void segment::writer::flush_chunk()
{
  segment_->chunks_.emplace_back(std::move(chunk_));
  chunk_ = chunk();
  putter_ = std::move(chunk::putter(&chunk_));
}


segment::reader::reader(segment const* s)
  : segment_(s)
  , chunk_(segment_->chunks_.begin())
  , getter_(&cppa::get<0>(segment_->chunks_.at(0)))
{
  assert(! segment_->chunks_.empty());
  ++chunk_;
}

segment::reader::reader(reader&& other)
  : bytes_(other.bytes_)
  , total_bytes_(other.total_bytes_)
  , segment_(other.segment_)
  , chunk_(std::move(other.chunk_))
  , getter_(std::move(other.getter_))
{
  other.bytes_ = 0;
  other.total_bytes_ = 0;
  other.segment_ = nullptr;
}

size_t segment::reader::bytes() const
{
  return total_bytes_ + bytes_;
}

uint32_t segment::reader::events() const
{
  return getter_.available();
}

size_t segment::reader::chunks() const
{
  return segment_->chunks_.end() - chunk_;
}

uint32_t segment::reader::operator>>(ze::event& e)
{
  if (events() == 0)
  {
    if (chunk_ == segment_->chunks_.end())
      throw error::segment("attempted read on invalid chunk");

    getter_ = std::move(chunk::getter(&cppa::get<0>(*chunk_++)));
    total_bytes_ += bytes_;
    bytes_ = 0;
  }

  bytes_ = getter_ >> e;
  return getter_.available();
}


segment::segment(ze::uuid uuid, ze::compression method)
{
  header_.id = std::move(uuid);
  header_.version = version;
  header_.compression = method;
  header_.start = ze::now();
  header_.end = header_.start;
}

segment::segment(segment const& other)
  : header_(other.header_)
  , chunks_(other.chunks_)
{
  DBG(core) << "copied a segment!";
}

segment::segment(segment&& other)
  : header_(std::move(other.header_))
  , chunks_(std::move(other.chunks_))
{
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
  return header_.events;
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

bool operator==(segment const& x, segment const& y)
{
  return x.header_.version == y.version &&
    x.header_.compression == y.header_.compression &&
    x.header_.start == y.header_.start &&
    x.header_.event_names == y.header_.event_names &&
    x.header_.events == y.header_.events &&
    x.chunks_ == y.chunks_;
}

bool operator!=(segment const& x, segment const& y)
{
  return ! (x == y);
}

} // namespace vast
