#include <vast/store/segment.h>

#include <ze/event.h>
#include <ze/util/make_unique.h>
#include <vast/util/logger.h>

namespace vast {
namespace store {

// FIXME: Why does the linker complain without these definitions? These are
// redundant to those in the header file.
uint32_t const segment::magic;
uint8_t const segment::version;

segment::writer::writer(segment& s)
  : chunk_creator(s)
  , segment_(s)
  , putter_(segment_.chunks_.back().put())
{
}

void segment::writer::new_chunk()
{
  segment_.chunks_.emplace_back();
  putter_ = std::move(segment_.chunks_.back().put());
}

uint32_t segment::writer::operator<<(ze::event const& event)
{
  ++segment_.events_;

  if (event.timestamp() < segment_.start_)
    segment_.start_ = event.timestamp();
  if (event.timestamp() > segment_.end_)
    segment_.end_ = event.timestamp();

  auto i = std::lower_bound(segment_.event_names_.begin(),
                            segment_.event_names_.end(),
                            event.name());

  if (i == segment_.event_names_.end())
    segment_.event_names_.push_back(event.name());
  else if (event.name() < *i)
    segment_.event_names_.insert(i, event.name());

  bytes_ += putter_ << event;

  return segment_.chunks_.back().elements();
}

size_t segment::writer::bytes() const
{
  return bytes_;
}


segment::reader::reader(segment::chunk_type const& chunk)
  : getter_(chunk.get())
{
}

uint32_t segment::reader::operator>>(ze::event& e)
{
  bytes_ += getter_ >> e;
  return getter_.available();
}

size_t segment::reader::read(std::function<void(ze::event)> f)
{
  return getter_.get(f);
}

size_t segment::reader::bytes() const
{
  return bytes_;
}

segment::writer segment::write()
{
  return {*this};
}

segment::reader segment::read(size_t i) const
{
  assert(! chunks_.empty());
  assert(i < chunks_.size());
  return {chunks_[i]};
}


uint32_t segment::events() const
{
  return events_;
}

size_t segment::chunks() const
{
  return chunks_.size();
}

segment::segment(ze::compression method)
  : version_(version)
  , compression_(method)
  , events_(0)
{
  start_ = ze::clock::now();
  end_ = start_;
}

size_t segment::bytes() const
{
  // FIXME: compute incrementally rather than ad-hoc.
  static size_t constexpr header =
    sizeof(version_) +
    sizeof(compression_) +
    sizeof(start_) +
    sizeof(end_) +
    sizeof(events_);

  // FIXME: do not hardcode size of ze::serialization.
  size_t events = 8;
  for (auto& str : event_names_)
    events += 4 + str.size();

  size_t chunks = 8;
  for (auto& chk : chunks_)
    chunks += 4 + 8 + chk.size();

  return header + events + chunks;
}

} // namespace store
} // namespace vast
