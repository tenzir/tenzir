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

char const* event_meta_index::name() const
{
  return "event-meta-index";
}

void event_meta_index::load()
{
  io::unarchive(dir_ / "timestamp.idx", timestamp_);
  io::unarchive(dir_ / "name.idx", name_);
  VAST_LOG_ACT_DEBUG(name(), "loaded timestamp/name index with " <<
                     timestamp_.size() << '/' << name_.size() << " events");
}

void event_meta_index::store()
{
  io::archive(dir_ / "timestamp.idx", timestamp_);
  io::archive(dir_ / "name.idx", name_);
  VAST_LOG_ACT_DEBUG(name(), "stored timestamp/name index with " <<
                     timestamp_.size() << '/' << name_.size() << " events");
}

bool event_meta_index::index(event const& e)
{
  return timestamp_.push_back(e.timestamp(), e.id())
      && name_.push_back(e.name(), e.id());
}

option<bitstream> event_meta_index::lookup(expression const&)
{
  // TODO: Implement this function.
  return {};
}


event_arg_index::event_arg_index(path dir)
  : event_index<event_arg_index>{std::move(dir)}
{
}

char const* event_arg_index::name() const
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
      VAST_LOG_ACT_ERROR(name(), "invalid offset: " << p.basename());
      quit();
    }

    value_type vt;
    std::shared_ptr<bitmap_index> bmi;
    io::unarchive(p, vt, bmi);
    if (! bmi)
    {
      VAST_LOG_ACT_ERROR(name(), "got corrupt index: " << p.basename());
      quit();
    }
    VAST_LOG_ACT_DEBUG(name(), "read: " << p.trim(-3) << " with " <<
                      bmi->size() << " events");
    args_.emplace(o, bmi);
    types_[vt].push_back(bmi);
  }
}

void event_arg_index::store()
{
  VAST_LOG_ACT_DEBUG(name(), "saves indexes to filesystem");

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
    VAST_LOG_ACT_DEBUG(name(), "wrote index " << filename.trim(-3) <<
                       " with " << p.second->size() << " events");
  }
}

bool event_arg_index::index(event const& e)
{
  if (e.empty())
    return true;
  offset o{0};
  return index_impl(e, e.id(), o);
}

option<bitstream> event_arg_index::lookup(expression const&)
{
  // TODO: Implement this function.
  return {};
}

bool event_arg_index::index_impl(record const& r, uint64_t id, offset& o)
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
        if (! index_impl(inner, id, o))
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

} // namespace vast
