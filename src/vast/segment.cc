#include "vast/segment.h"

#include <caf/message_handler.hpp>
#include "vast/event.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/serialization/flat_set.h"
#include "vast/serialization/variant.h"

namespace vast {

uint32_t const segment::meta_data::magic;
uint32_t const segment::meta_data::version;

bool segment::meta_data::contains(event_id eid) const
{
  return base != 0 && base <= eid && eid < base + events;
}

bool segment::meta_data::contains(event_id from, event_id to) const
{
  return base != 0 && from < to && base <= from && to < base + events;
}

void segment::meta_data::serialize(serializer& sink) const
{
  sink
    << magic << version
    << id
    << first
    << last
    << base
    << events
    << bytes
    << schema;
}

void segment::meta_data::deserialize(deserializer& source)
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
    >> first
    >> last
    >> base
    >> events
    >> bytes
    >> schema;
}

bool operator==(segment::meta_data const& x, segment::meta_data const& y)
{
  return x.id == y.id
      && x.first == y.first
      && x.last == y.last
      && x.base == y.base
      && x.events == y.events
      && x.bytes == y.bytes
      && x.schema == y.schema;
}

segment::reader::reader(segment const& s)
  : segment_{&s},
    next_{segment_->meta_.base},
    chunk_base_{segment_->meta_.base}
{
  if (! segment_->chunks_.empty())
  {
    current_ = &segment_->chunks_.front();
    chunk_reader_ = std::make_unique<chunk::reader>(*current_);
  }
}

event_id segment::reader::position() const
{
  return next_;
}

result<event> segment::reader::read(event_id id)
{
  if (! chunk_reader_)
    return error{"cannot read from empty segment"};

  if (id > 0)
  {
    if (! segment_->meta_.contains(id))
    {
      return error{"event ID ", id, " out of bounds"};
    }
    else if (id < next_)
    {
      if (id >= chunk_base_)
      {
        next_ = chunk_base_;
        chunk_reader_ = std::make_unique<chunk::reader>(*current_);
      }
      else
      {
        while (id < next_)
          if (! prev())
            return error{"backward seek failure"};
      }
    }
    else if (id > next_)
    {
      while (id >= chunk_base_ + current_->events())
        if (! next())
          return error{"forward seek failure"};
    }

    next_ = id;
  }

  if (next_ - chunk_base_ == current_->events() && ! next())
    return error{"processed all chunks"};

  assert(next_ >= chunk_base_);
  auto e = chunk_reader_->read(next_ - chunk_base_);
  ++next_;

  return e;
}

chunk const* segment::reader::prev()
{
  if (segment_->chunks_.empty() || chunk_idx_ == 0)
    return nullptr;

  current_ = &segment_->chunks_[--chunk_idx_];
  chunk_reader_ = std::make_unique<chunk::reader>(*current_);

  if (next_ > 0)
  {
    chunk_base_ -= current_->events();
    next_ = chunk_base_;
  }

  return current_;
}

chunk const* segment::reader::next()
{
  if (! current_ || chunk_idx_ + 1 == segment_->chunks_.size())
    return nullptr;

  if (next_ > 0)
  {
    chunk_base_ += current_->events();
    next_ = chunk_base_;
  }

  current_ = &segment_->chunks_[++chunk_idx_];
  chunk_reader_ = std::make_unique<chunk::reader>(*current_);

  return current_;
}


segment::segment(uuid id)
{
  meta_.id = id;
}

trial<void> segment::push_back(chunk chk)
{
  if (chk.events() == 0)
    return error{"empty chunk"};

  // The first chunk determines the segment base.
  if (chunks_.empty())
  {
    auto first = chk.meta().ids.find_first();
    if (first != ewah_bitstream::npos)
      meta_.base = first;
  }
  else if (meta_.base != 0)
  {
    // If they have a base, segments must comprise chunks with adjacent
    // sequential ID ranges.
    auto first = chk.meta().ids.find_first();
    if (meta_.base + meta_.events != first)
      return error{"expected chunk with first event ID ",
                   meta_.base + meta_.events, ", got ", first};
  }

  meta_.events += chk.events();

  if (chk.meta().first < meta_.first)
    meta_.first = chk.meta().first;

  if (chk.meta().last > meta_.last)
    meta_.last = chk.meta().last;

  auto s = schema::merge(chk.meta().schema, meta_.schema);
  if (s)
    meta_.schema = std::move(*s);
  else
    return s.error();

  chunks_.push_back(std::move(chk));

  return nothing;
}

segment::meta_data const& segment::meta() const
{
  return meta_;
}

bool segment::empty() const
{
  return size() == 0;
}

size_t segment::size() const
{
  return chunks_.size();
}

void segment::serialize(serializer& sink) const
{
  sink << meta_ << chunks_;
}

void segment::deserialize(deserializer& source)
{
  source >> meta_ >> chunks_;
}

bool operator==(segment const& x, segment const& y)
{
  return x.meta_ == y.meta_;
}

} // namespace vast
