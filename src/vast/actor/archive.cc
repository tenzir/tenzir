#include <caf/all.hpp>

#include "vast/actor/archive.h"
#include "vast/concept/serializable/chunk.h"
#include "vast/io/serialization.h"

namespace vast {

using namespace caf;

archive::archive(path dir, size_t capacity, size_t max_segment_size)
  : flow_controlled_actor{"archive"},
    dir_{dir / "archive"},
    meta_data_filename_{dir_ / "meta.data"},
    max_segment_size_{max_segment_size},
    cache_{capacity}
{
  assert(max_segment_size_ > 0);
  trap_exit(true);
}

void archive::on_exit()
{
  accountant_ = invalid_actor;
}

caf::behavior archive::make_behavior()
{
  if (exists(meta_data_filename_))
  {
    auto t = io::unarchive(meta_data_filename_, segments_);
    if (! t)
    {
      VAST_ERROR(this, "failed to unarchive meta data:", t.error());
      quit(exit::error);
      return {};
    }
  }
  return
  {
    register_upstream_node(),
    [=](exit_msg const& msg)
    {
      if (downgrade_exit())
        return;
      if (! current_.empty())
      {
        auto t = store(std::move(current_));
        if (! t)
        {
          VAST_ERROR(this, "failed to store segment:", t.error());
          return;
        }
      }
      VAST_VERBOSE(this, "writes meta data to:", meta_data_filename_.trim(-3));
      auto t = io::archive(meta_data_filename_, segments_);
      if (! t)
      {
        VAST_ERROR(this, "failed to store segment meta data:", t.error());
        return;
      }
      quit(msg.reason);
    },
    [=](down_msg const& msg)
    {
      remove_upstream_node(msg.source);
    },
    [=](accountant_atom, actor const& accountant)
    {
      VAST_DEBUG(this, "registers accountant", accountant);
      accountant_ = accountant;
      send(accountant_, label() + "-events", time::now());
    },
    [=](chunk const& chk)
    {
      VAST_DEBUG(this, "got chunk [" << chk.meta().ids.find_first() << ',' <<
                 (chk.meta().ids.find_last() + 1 ) << ')');
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
      if (accountant_)
        send(accountant_, chk.events(), time::snapshot());
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
      return make_message(empty_atom::value, eid);
    },
    catch_unexpected()
  };
}

trial<void> archive::store(segment s)
{
  if (! exists(dir_) && ! mkdir(dir_))
    return error{"failed to create directory ", dir_};
  auto id = uuid::random();
  auto filename = dir_ / to_string(id);
  VAST_VERBOSE(this, "writes segment", id, "to", filename.trim(-3));
  auto t = io::archive(filename, s);
  if (! t)
    return t;
  for (auto& chk : s)
  {
    auto first = chk.meta().ids.find_first();
    auto last = chk.meta().ids.find_last();
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
    auto s = cache_.lookup(*id);
    if (s == nullptr)
    {
      VAST_DEBUG(this, "experienced cache miss for", *id);
      segment seg;
      auto filename = dir_ / to_string(*id);
      auto t = io::unarchive(filename, seg);
      if (! t)
      {
        VAST_ERROR(this, "failed to unarchive segment:", t.error());
        return t.error();
      }
      s = cache_.insert(*id, std::move(seg)).first;
    }
    for (size_t i = 0; i < s->size(); ++i)
      if (eid < (*s)[i].meta().ids.size() && (*s)[i].meta().ids[eid])
        return (*s)[i];
    assert(! "segment must contain looked up id");
  }
  return error{"no segment for id ", eid};
}

} // namespace vast
