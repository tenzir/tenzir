#include "vast/archive.h"

#include <caf/all.hpp>
#include "vast/file_system.h"
#include "vast/serialization/all.h"
#include "vast/io/serialization.h"

namespace vast {

using namespace caf;

archive::archive(path dir, size_t capacity, size_t max_segment_size)
  : dir_{dir / "archive"},
    max_segment_size_{max_segment_size},
    cache_{capacity, [&](uuid const& id) { return on_miss(id); }}
{
  assert(max_segment_size_ > 0);
}

caf::message_handler archive::act()
{
  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        segment_files_.emplace(to_string(p.basename()), p);

        segment::meta_data meta;
        io::unarchive(p, meta);
        VAST_LOG_DEBUG("found segment " << p.basename() <<
                       " for ID range [" << meta.base << ", " <<
                       meta.base + meta.events << ")");

        if (! ranges_.insert(meta.base, meta.base + meta.events, meta.id))
        {
          VAST_LOG_ERROR("inconsistency in ID space for [" <<
                         meta.base << ", " << meta.base + meta.events << ")");
          return false;
        }

        return true;
      });

  attach_functor(
    [=](uint32_t)
    {
      if (current_size_ == 0)
        return;

      VAST_LOG_ACTOR_VERBOSE("writes buffered segment to disk");
      if (! store(make_message(std::move(current_))))
        VAST_LOG_ACTOR_ERROR("failed to save buffered segment");
    });

  return
  {
    [=](chunk const& chk)
    {
      if (! current_.empty()
          && current_size_ + chk.bytes() >= max_segment_size_)
      {
        if (! store(make_message(std::move(current_))))
        {
          VAST_LOG_ACTOR_ERROR("failed to save buffered segment");
          quit(exit::error);
          return;
        }

        current_ = {};
        current_size_ = 0;
      }

      current_size_ += chk.bytes();
      current_.push_back(chk);
    },
    [=](event_id eid)
    {
      auto t = load(eid);
      if (t)
      {
        VAST_LOG_ACTOR_DEBUG("delivers segment for event " << eid);
        return *t;
      }
      else
      {
        VAST_LOG_ACTOR_WARN(t.error());
        return make_message(atom("no segment"), eid);
      }
    }
  };
}

std::string archive::describe() const
{
  return "archive";
}

bool archive::store(message msg)
{
  if (! exists(dir_) && ! mkdir(dir_))
  {
    VAST_LOG_ERROR("failed to create directory " << dir_);
    return false;
  }

  msg.apply(
      on_arg_match >> [&](segment const& s)
      {
        assert(segment_files_.find(s.meta().id) == segment_files_.end());

        auto filename = dir_ / path{to_string(s.meta().id)};

        auto t = io::archive(filename, s);
        if (t)
          VAST_LOG_VERBOSE("wrote segment " << s.meta().id << " to " <<
                           filename);
        else
          VAST_LOG_ERROR(t.error());

        segment_files_.emplace(s.meta().id, filename);
        cache_.insert(s.meta().id, msg);
        ranges_.insert(s.meta().base,
                       s.meta().base + s.meta().events,
                       s.meta().id);
      });

  return true;
}

trial<message> archive::load(event_id eid)
{
  if (auto id = ranges_.lookup(eid))
    return cache_.retrieve(*id);
  else
    return error{"no segment for id ", eid};
}

message archive::on_miss(uuid const& id)
{
  VAST_LOG_DEBUG("experienced cache miss for " << id);
  assert(segment_files_.find(id) != segment_files_.end());

  segment s;
  io::unarchive(dir_ / path{to_string(id)}, s);

  return make_message(std::move(s));
}

} // namespace vast
