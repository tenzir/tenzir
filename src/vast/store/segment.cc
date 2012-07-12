#include "vast/store/segment.h"

#include <ze/event.h>
#include <ze/util/make_unique.h>
#include "vast/util/logger.h"

namespace vast {
namespace store {

segment::writer::writer(segment& s)
  : chunk_creator(s)
  , putter_(segment_.chunks_.back().put())
{
}

uint32_t segment::writer::operator<<(ze::event const& event)
{
  ++segment_.n_events_;

  if (event.timestamp() < segment_.start_)
    segment_.start_ = event.timestamp();
  if (event.timestamp() > end_)
    segment_.end_ = event.timestamp();

  auto i = std::lower_bound(segment_.events_.begin(),
                            segment_.events_.end(),
                            event.name());

  if (i == events_.end())
    segment_.events_.push_back(event.name());
  else if (event.name() < *i)
    segment_.events_.insert(i, event.name());

  bytes_ += putter_ << event;

  return segment_.chunks_.back().elements();
}

size_t segment::writer::bytes() const
{
  return bytes_;
}


segment::reader::reader(segment& s)
  : getter(segment_.chunks_.back().put())
{
}

uint32_t segment::reader::operator>>(ze::event& e)
{
  bytes_ += getter_ >> e;
  return getter_.available();
}

size_t segment::reader::read(std::function<void(ze::event)> f)
{
  //try
  //{
  //  (**current_).get(f);
  //}
  //catch (ze::bad_type_exception e)
  //{
  //  LOG(error, store) << "skipping rest of bad chunk: " << e.what();
  //}
  //catch (ze::serialization::exception const& e)
  //{
  //  LOG(error, store) << "error while deserializing events: " << e.what();
  //  LOG(error, store)
  //    << "skipping rest of chunk #"
  //    << (current_ - chunks_.begin());
  //}

  return getter_.get(f);
}

size_t segment::reader::bytes() const
{
  return bytes_;
}

segment::writer write()
{
  return {*this};
}

segment::reader read(size_t i) const
{
  assert(! chunks_.empty());
  assert(i < chunks_.size());
  return {*this, chunks_[i]};
}


uint32_t segment::n_events() const
{
  return n_events_;
}

size_t segment::chunks() const
{
  return chunks_.size();
}

segment::segment(ze::compression method)
  : version_(version)
  , compression_(method)
  , n_events_(0)
{
  start_ = ze::clock::now();
  end_ = start_;
}

segment::segment(segment&& other)
  : object(std::move(other))
  , version_(other.version_)
  , compression_(other.compression_)
  , start_(std::move(other.start_))
  , end_(std::move(other.end_))
  , events_(std::move(other.events_))
  , n_events_(other.n_events_)
{
  other.version_ = 0;
  other.n_events_ = 0;
}

size_t segment::bytes() const
{
  // FIXME: compute incrementally rather than ad-hoc.
  static size_t constexpr header =
    sizeof(version_) +
    sizeof(method_) +
    sizeof(start_) +
    sizeof(end_) +
    sizeof(n_events_);

  // FIXME: do not hardcode size of ze::serialization.
  size_t events = 8;
  for (auto& str : event_)
    events += 4 + str.size();

  size_t chunks = 8;
  for (auto& chk : chunks_)
    chunks += 4 + 8 + chk.size();

  return header + events + chunks;
}

} // namespace store
} // namespace vast
