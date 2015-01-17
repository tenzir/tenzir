#include "vast/actor/archive.h"

#include <caf/all.hpp>
#include "vast/serialization/flat_set.h"
#include "vast/serialization/range_map.h"
#include "vast/io/serialization.h"

namespace vast {

using namespace caf;

archive::archive(path dir, size_t capacity, size_t max_segment_size)
  : dir_{dir / "archive"},
    max_segment_size_{max_segment_size},
    cache_{capacity, [&](uuid const& id) { return on_miss(id); }}
{
  assert(max_segment_size_ > 0);

  attach_functor(
    [=](uint32_t)
    {
      if (! current_.empty())
      {
        auto t = store(std::move(current_));
        if (! t)
        {
          VAST_ERROR(this, "failed to store segment:", t.error());
          return;
        }
      }

      auto t = io::archive(dir_ / "meta.data", segments_);
      if (! t)
      {
        VAST_ERROR(this, "failed to store segment meta data:", t.error());
        return;
      }
    });
}

caf::message_handler archive::make_handler()
{
  if (exists(dir_ / "meta.data"))
  {
    auto t = io::unarchive(dir_ / "meta.data", segments_);
    if (! t)
    {
      VAST_ERROR(this, "failed to unarchive meta data:", t.error());
      quit(exit::error);
      return {};
    }
  }

  return
  {
    [=](chunk const& chk)
    {
      if (! current_.empty()
          && current_size_ + chk.bytes() >= max_segment_size_)
      {
        auto t = store(std::move(current_));
        if (! t)
        {
          VAST_ERROR(this, "failed to store segment:", t.error());
          quit(exit::error);
          return;
        }

        current_ = {};
        current_size_ = 0;
      }

      current_size_ += chk.bytes();
      current_.insert(chk);
    },
    [=](event_id eid)
    {
      VAST_DEBUG(this, "got request for event", eid);

      // First check the currently buffered segment.
      for (size_t i = 0; i < current_.size(); ++i)
        if (eid < current_[i].meta().ids.size() && current_[i].meta().ids[eid])
          return make_message(current_[i]);

      // Then inspect the existing segments.
      auto t = load(eid);
      if (t)
      {
        VAST_DEBUG(this, "delivers chunk for event", eid);
        return make_message(*t);
      }

      VAST_WARN(this, t.error());
      return make_message(atom("no chunk"), eid);
    }
  };
}

std::string archive::name() const
{
  return "archive";
}

trial<void> archive::store(segment s)
{
  if (! exists(dir_) && ! mkdir(dir_))
    return error{"failed to create directory ", dir_};

  auto id = uuid::random();
  auto filename = dir_ / to_string(id);
  VAST_VERBOSE(this, "writes segment", id, "to", filename);
  auto t = io::archive(filename, s);
  if (! t)
    return t;

  for (auto& chk : s)
  {
    auto first = chk.meta().ids.find_first();
    auto last = chk.meta().ids.find_last();
    assert(first != last);
    assert(first != invalid_event_id && last != invalid_event_id);
    segments_.inject(first, last + 1, id);
  }

  cache_.insert(std::move(id), std::move(s));

  return nothing;
}

trial<chunk> archive::load(event_id eid)
{
  if (auto id = segments_.lookup(eid))
  {
    auto& s = cache_.retrieve(*id);
    for (size_t i = 0; i < s.size(); ++i)
      if (eid < s[i].meta().ids.size() && s[i].meta().ids[eid])
        return s[i];

    assert(! "segment must contain looked up id");
  }

  return error{"no segment for id ", eid};
}

archive::segment archive::on_miss(uuid const& id)
{
  VAST_DEBUG(this, "experienced cache miss for", id);

  segment s;
  auto filename = dir_ / to_string(id);
  auto t = io::unarchive(filename, s);
  if (! t)
  {
    VAST_ERROR(this, "failed to unarchive segment:", t.error());
    return {};
  }

  return s;
}

} // namespace vast
