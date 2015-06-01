#include <caf/all.hpp>

#include "vast/chunk.h"
#include "vast/event.h"
#include "vast/actor/archive.h"
#include "vast/concept/serializable/chunk.h"
#include "vast/concept/serializable/io.h"
#include "vast/util/assert.h"

namespace vast {

using namespace caf;

archive::archive(path dir, size_t capacity, size_t max_segment_size,
                 io::compression compression)
  : flow_controlled_actor{"archive"},
    dir_{dir},
    meta_data_filename_{dir_ / "meta.data"},
    max_segment_size_{max_segment_size},
    compression_{compression},
    cache_{capacity}
{
  VAST_ASSERT(max_segment_size_ > 0);
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
    auto t = load(meta_data_filename_, segments_);
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
      if (! flush())
        return;
      quit(msg.reason);
    },
    [=](down_msg const& msg)
    {
      remove_upstream_node(msg.source);
    },
    [=](put_atom, accountant_atom, actor const& accountant)
    {
      VAST_DEBUG(this, "registers accountant", accountant);
      accountant_ = accountant;
      send(accountant_, label() + "-events", time::now());
    },
    [=](std::vector<event> const& events)
    {
      VAST_DEBUG(this, "got", events.size(), "events [" <<
                 events.front().id() << ',' << (events.back().id() + 1) << ')');
      chunk chk{events, compression_};
      auto too_large = current_size_ + chk.bytes() >= max_segment_size_;
      if (! current_.empty() && too_large && ! flush())
        return;
      if (accountant_)
        send(accountant_, uint64_t{events.size()}, time::snapshot());
      current_size_ += chk.bytes();
      current_.insert(std::move(chk));
    },
    [=](flush_atom)
    {
      if (flush())
        return make_message(ok_atom::value);
      else
        return make_message(error{"flush failed"});
    },
    [=](event_id eid)
    {
      VAST_DEBUG(this, "got request for event", eid);
      // First check the currently buffered segment.
      for (size_t i = 0; i < current_.size(); ++i)
        if (eid < current_[i].meta().ids.size() && current_[i].meta().ids[eid])
          return make_message(current_[i]);
      // Then inspect the existing segments.
      if (auto id = segments_.lookup(eid))
      {
        auto s = cache_.lookup(*id);
        if (s == nullptr)
        {
          VAST_DEBUG(this, "experienced cache miss for", *id);
          segment seg;
          auto filename = dir_ / to_string(*id);
          auto t = load(filename, seg);
          if (! t)
          {
            VAST_ERROR(this, "failed to unarchive segment:", t.error());
            quit(exit::error);
            return make_message(empty_atom::value, eid);
          }
          s = cache_.insert(*id, std::move(seg)).first;
        }
        for (size_t i = 0; i < s->size(); ++i)
          if (eid < (*s)[i].meta().ids.size() && (*s)[i].meta().ids[eid])
          {
            VAST_DEBUG(this, "delivers chunk for event", eid);
            return make_message((*s)[i]);
          }
        VAST_ASSERT(! "segment must contain looked up id");
      }
      VAST_WARN(this, "no segment for id", eid);
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
  auto t = save(filename, s);
  if (! t)
    return t;
  for (auto& chk : s)
  {
    auto first = chk.meta().ids.find_first();
    auto last = chk.meta().ids.find_last();
    VAST_ASSERT(first != invalid_event_id && last != invalid_event_id);
    segments_.inject(first, last + 1, id);
  }
  cache_.insert(std::move(id), std::move(s));
  return nothing;
}

bool archive::flush()
{
  VAST_VERBOSE(this, "flushes segment with", current_.size(), "chunks");
  if (current_.empty())
    return true;
  auto t = store(std::move(current_));
  if (! t)
  {
    VAST_ERROR(this, "failed to store segment:", t.error());
    quit(exit::error);
    return false;
  }
  current_ = {};
  current_size_ = 0;
  VAST_VERBOSE(this, "writes meta data to:", meta_data_filename_.trim(-3));
  t = save(meta_data_filename_, segments_);
  if (! t)
  {
    VAST_ERROR(this, "failed to store segment meta data:", t.error());
    quit(exit::error);
    return false;
  }
  return true;
}

} // namespace vast
