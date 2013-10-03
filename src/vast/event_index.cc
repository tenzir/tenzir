#include "vast/event_index.h"

#include "vast/bitmap_index/address.h"
#include "vast/bitmap_index/arithmetic.h"
#include "vast/bitmap_index/port.h"
#include "vast/convert.h"

using namespace cppa;

namespace vast {

event_meta_index::event_meta_index(path dir)
  : event_index<event_meta_index>{std::move(dir)}
{
}

char const* event_meta_index::description() const
{
  return "event-meta-index";
}

void event_meta_index::load()
{
  io::unarchive(dir_ / "timestamp.idx", timestamp_);
  io::unarchive(dir_ / "name.idx", name_);
  VAST_LOG_ACTOR_DEBUG("loaded timestamp/name index with " <<
                     timestamp_.size() << '/' << name_.size() << " events");
}

void event_meta_index::store()
{
  io::archive(dir_ / "timestamp.idx", timestamp_);
  io::archive(dir_ / "name.idx", name_);
  VAST_LOG_ACTOR_DEBUG("stored timestamp/name index with " <<
                       timestamp_.size() << '/' << name_.size() << " events");
}

bool event_meta_index::index(event const& e)
{
  return timestamp_.push_back(e.timestamp(), e.id())
      && name_.push_back(e.name(), e.id());
}

behavior event_meta_index::actor_behavior() const
{
  return (
      on(atom("lookup"), atom("time"), arg_match)
        >> [=](relational_operator op, value const& v, actor_ptr sink)
      {
        send(sink, atom("result"), atom("expected"), self);
        if (auto result = timestamp_.lookup(op, v))
          send(sink, atom("result"), std::move(*result));
        else
          send(sink, atom("result"), atom("miss"));
      });
}


event_arg_index::event_arg_index(path dir)
  : event_index<event_arg_index>{std::move(dir)}
{
}

char const* event_arg_index::description() const
{
  return "event-arg-index";
}

void event_arg_index::load()
{
  std::set<path> paths;
  traverse(dir_, [&](path const& p) -> bool { paths.insert(p); return true; });
  for (auto& p : paths)
  {
    offset o;
    auto str = p.basename(true).str().substr(1);
    auto start = str.begin();
    if (! extract(start, str.end(), o))
    {
      VAST_LOG_ACTOR_ERROR("got invalid offset: " << p.basename());
      quit(exit::error);
      return;
    }

    value_type vt;
    std::shared_ptr<bitmap_index> bmi;
    io::unarchive(p, vt, bmi);
    if (! bmi)
    {
      VAST_LOG_ACTOR_ERROR("got corrupt index: " << p.basename());
      quit(exit::error);
      return;
    }
    VAST_LOG_ACTOR_DEBUG("read: " << p.trim(-3) << " with " <<
                         bmi->size() << " events");
    args_.emplace(o, bmi);
    types_[vt].push_back(bmi);
  }
}

void event_arg_index::store()
{
  VAST_LOG_ACTOR_DEBUG("saves indexes to filesystem");

  std::map<std::shared_ptr<bitmap_index>, value_type> inverse;
  for (auto& p : types_)
    for (auto& bmi : p.second)
      if (inverse.find(bmi) == inverse.end())
        inverse.emplace(bmi, p.first);

  static string prefix{"@"};
  static string suffix{".idx"};
  for (auto& p : args_)
  {
    if (p.second->empty())
      continue;
    path const filename = dir_ / (prefix + to<string>(p.first) + suffix);
    assert(inverse.count(p.second));
    io::archive(filename, inverse[p.second], p.second);
    VAST_LOG_ACTOR_DEBUG("wrote index " << filename.trim(-3) <<
                         " with " << p.second->size() << " events");
  }
}

bool event_arg_index::index(event const& e)
{
  if (e.empty())
    return true;
  offset o{0};
  return index_record(e, e.id(), o);
}

behavior event_arg_index::actor_behavior() const
{
  return (
      on(atom("lookup"), atom("type"), arg_match)
        >> [=](relational_operator op, value const& v, actor_ptr sink)
      {
        send(sink, atom("result"), atom("expected"), self);
        if (auto result = type_lookup(op, v))
          send(sink, atom("result"), std::move(*result));
        else
          send(sink, atom("result"), atom("miss"));
      },
      on(atom("lookup"), atom("offset"), arg_match)
        >> [=](relational_operator op, value const& v,
               offset const& off, actor_ptr sink)
      {
        send(sink, atom("result"), atom("expected"), self);
        if (auto result = offset_lookup(op, v, off))
          send(sink, atom("result"), std::move(*result));
        else
          send(sink, atom("result"), atom("miss"));
      });
}

bool event_arg_index::index_record(record const& r, uint64_t id, offset& o)
{
  if (o.empty())
    return true;
  for (auto& v : r)
  {
    if (v.which() == record_type && v)
    {
      auto& inner = v.get<record>();
      if (! inner.empty())
      {
        o.push_back(0);
        if (! index_record(inner, id, o))
          return false;
        o.pop_back();
      }
    }
    else if (! v.invalid() && v.which() != table_type)
    {
      bitmap_index* idx;
      auto i = args_.find(o);
      if (i != args_.end())
      {
        idx = i->second.get();
      }
      else
      {
        auto unique = bitmap_index::create(v.which());
        auto bmi = std::shared_ptr<bitmap_index>{unique.release()};
        idx = bmi.get();
        args_.emplace(o, bmi);
        types_[v.which()].push_back(bmi);
      }
      assert(idx != nullptr);
      if (! idx->push_back(v, id))
        return false;
    }
    ++o.back();
  }
  return true;
}

optional<bitstream> event_arg_index::type_lookup(
    relational_operator op, value const& v) const
{
  bitstream result;
  auto i = types_.find(v.which());
  if (i == types_.end())
    return {};
  for (auto& bmi : i->second)
    if (auto r = bmi->lookup(op, v))
    {
      if (result)
        result |= *r;
      else
        result = std::move(*r);
    }
  if (! result || result.empty())
    return {};
  else
    return {std::move(result)};
}

optional<bitstream> event_arg_index::offset_lookup(
    relational_operator op, value const& v, offset const& o) const
{
  auto i = args_.find(o);
  if (i == args_.end())
    return {};
  auto& bmi = i->second;
  assert(bmi != nullptr);
  if (auto r = bmi->lookup(op, v))
    return r;
  else
    return {};
}

} // namespace vast
