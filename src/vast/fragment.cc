#include "vast/fragment.h"

#include "vast/logger.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/io/serialization.h"

using namespace cppa;

namespace vast {

fragment::fragment(path dir)
  : dir_{std::move(dir)}
{
}

void fragment::init()
{
  VAST_LOG_ACT_VERBOSE("fragment", "spawned");

  if (exists(dir_))
    load();
  else
    mkdir(dir_);

  become(
      on(atom("kill")) >> [=]
      {
        store();
        quit();
      },
      on(atom("store")) >> [=]
      {
        store();
      },
      on_arg_match >> [=](event const& e)
      {
        index(e);
      },
      on_arg_match >> [=](expression const& e)
      {
        if (auto result = lookup(e))
          reply(std::move(*result));
        else
          reply(atom("miss"));
      });
}

void fragment::on_exit()
{
  VAST_LOG_ACT_VERBOSE("fragment", "terminated");
}

bool fragment::append_value(bitmap_index& bmi, uint64_t id, value const& val)
{
  if (id < bmi.size())
  {
    VAST_LOG_ACT_ERROR("fragment", "got event with incompatible ID: " << id);
    return false;
  }

  auto delta = id - bmi.size();
  if (delta > 1)
    if (! bmi.append(delta - 1, false))
      return false;

  return bmi.push_back(val);
}


meta_fragment::meta_fragment(path dir)
  : fragment{std::move(dir)}
{
}

void meta_fragment::load()
{
  VAST_LOG_ACT_DEBUG("meta-fragment", "reads indexes from filesystem");
  io::unarchive(dir_ / "timestamp.idx", timestamp_);
  io::unarchive(dir_ / "name.idx", name_);
  VAST_LOG_ACT_DEBUG("meta-fragment", "read timestamp/name index with " <<
                     timestamp_.size() << '/' << name_.size() << " events");
}

void meta_fragment::store()
{
  VAST_LOG_ACT_DEBUG("meta-fragment", "writes indexes to filesystem");
  io::archive(dir_ / "timestamp.idx", timestamp_);
  io::archive(dir_ / "name.idx", name_);
  VAST_LOG_ACT_DEBUG("meta-fragment", "stored timestamp/name index with " <<
                     timestamp_.size() << '/' << name_.size() << " events");
}

void meta_fragment::index(event const& e)
{
  if (! append_value(timestamp_, e.id(), e.timestamp()))
  {
    VAST_LOG_ACT_ERROR("fragment",
                       "failed to index event timestamp " << e.timestamp());
    quit();
  }

  if (! append_value(name_, e.id(), e.name()))
  {
    VAST_LOG_ACT_ERROR("fragment",
                       "failed to index event name " << e.name());
    quit();
  }
}

// TODO: Implement this function.
option<bitstream> meta_fragment::lookup(expression const&)
{
  return {};
}


type_fragment::type_fragment(path dir)
  : fragment{std::move(dir)}
{
}

void type_fragment::load()
{
  VAST_LOG_ACT_DEBUG("type-fragment", "reads indexes from filesystem");
  io::unarchive(dir_ / "bool.idx", bool_);
  io::unarchive(dir_ / "int.idx", int_);
  io::unarchive(dir_ / "uint.idx", uint_);
  io::unarchive(dir_ / "double.idx", double_);
  io::unarchive(dir_ / "time-range.idx", time_range_);
  io::unarchive(dir_ / "time-point.idx", time_point_);
  io::unarchive(dir_ / "string.idx", string_);
  io::unarchive(dir_ / "address.idx", address_);
  io::unarchive(dir_ / "port.idx", port_);
  VAST_LOG_ACT_DEBUG("type-fragment", "read indexes with " <<
                     bool_.size() << '/' <<
                     int_.size() << '/' <<
                     uint_.size() << '/' <<
                     double_.size() << '/' <<
                     time_range_.size() << '/' <<
                     time_point_.size() << '/' <<
                     string_.size() << '/' <<
                     address_.size() << '/' <<
                     port_.size() << " events");
}

void type_fragment::store()
{
  VAST_LOG_ACT_DEBUG("type-fragment", "writes indexes to filesystem");
  io::archive(dir_ / "bool.idx", bool_);
  io::archive(dir_ / "int.idx", int_);
  io::archive(dir_ / "uint.idx", uint_);
  io::archive(dir_ / "double.idx", double_);
  io::archive(dir_ / "time-range.idx", time_range_);
  io::archive(dir_ / "time-point.idx", time_point_);
  io::archive(dir_ / "string.idx", string_);
  io::archive(dir_ / "address.idx", address_);
  io::archive(dir_ / "port.idx", port_);
  VAST_LOG_ACT_DEBUG("type-fragment", "wrote indexes with " <<
                     bool_.size() << '/' <<
                     int_.size() << '/' <<
                     uint_.size() << '/' <<
                     double_.size() << '/' <<
                     time_range_.size() << '/' <<
                     time_point_.size() << '/' <<
                     string_.size() << '/' <<
                     address_.size() << '/' <<
                     port_.size() << " events");
}

void type_fragment::index(event const& e)
{
  std::set<value> unique;
  for (auto& v : e)
    if (! unique.count(v))
    {
      if (index_impl(e.id(), v))
        unique.insert(v);
      else
        VAST_LOG_ACT_ERROR("type-fragment", "failed to index value " << v <<
                           " in event: " << e);
    }
}

bool type_fragment::index_impl(uint64_t id, const value& v)
{
  switch (v.which())
  {
    default:
      VAST_LOG_ACT_ERROR("type-fragment",
                         "cannot index value type " << v.which());
      break;
    case bool_type:
      return append_value(bool_, id, v);
    case int_type:
      return append_value(int_, id, v);
    case uint_type:
      return append_value(uint_, id, v);
    case double_type:
      return append_value(double_, id, v);
    case time_range_type:
      return append_value(time_range_, id, v);
    case time_point_type:
      return append_value(time_point_, id, v);
    case string_type:
      return append_value(string_, id, v);
    case address_type:
      return append_value(address_, id, v);
    case port_type:
      return append_value(port_, id, v);
    case record_type:
      for (auto& rv : v.get<record>())
        if (! index_impl(id, rv))
          return false;
  }
  return true;
}

// TODO: Implement this function.
option<bitstream> type_fragment::lookup(expression const&)
{
  return {};
}

argument_fragment::argument_fragment(path dir)
  : fragment{std::move(dir)}
{
}

void argument_fragment::load()
{
  VAST_LOG_ACT_DEBUG("arg-fragment", "reads indexes from filesystem");
  std::set<path> paths;
  traverse(dir_, [&](path const& p) -> bool { paths.insert(p); return true; });

  for (auto& p : paths)
  {
    offset o;
    auto str = p.basename(true).str();
    assert(str[0] == '@');
    for (auto& pair : str.substr(1).split(','))
      o.push_back(to<size_t>(string{pair.first, pair.second}));

    std::unique_ptr<bitmap_index> bi;
    io::unarchive(p, bi);
    if (! bi)
    {
      VAST_LOG_ACT_ERROR("arg-fragment", "got corrupt index: " << p.basename());
      break;
    }
    VAST_LOG_ACT_DEBUG("arg-fragment", "read: " << p.trim(-3) << " with " <<
                      bi->size() << " events");
    indexes_.emplace(o, std::move(bi));
  }
}

void argument_fragment::store()
{
  VAST_LOG_ACT_DEBUG("arg-fragment", "writes indexes to filesystem");
  static string prefix{"@"};
  static string suffix{".idx"};
  for (auto& p : indexes_)
  {
    if (p.second->empty())
      continue;

    auto f = p.first.begin();
    auto l = p.first.end();
    string o;
    while (f != l)
    {
      o = o + to<string>(*f);
      if (++f != l)
        o = o + ',';
    }

    path const filename = dir_ / (prefix + o + suffix);
    io::archive(filename, p.second);
    VAST_LOG_ACT_DEBUG("arg-fragment", "wrote index " << filename.trim(-3) <<
                       " with " << p.second->size() << " events");
  }
}

void argument_fragment::index(event const& e)
{
  if (e.empty())
    return;
  offset o{0};
  if (! index_impl(e, e.id(), o))
    VAST_LOG_ACT_ERROR("arg-fragment",
                       "failed to index arguments of event: " << e);
}

// TODO: Implement this function.
option<bitstream> argument_fragment::lookup(expression const&)
{
  return {};
}

bool argument_fragment::index_impl(record const& r, uint64_t id, offset& o)
{
  if (o.empty())
    return true;

  for (auto& v : r)
  {
    if (v.which() == invalid_type || is_container_type(v.which()))
    {
      // TODO: support container types.
      ++o.back();
      continue;
    }
    if (v.which() == record_type)
    {
      if (v.nil())
        continue;
      auto& inner = v.get<record>();
      if (! inner.empty())
      {
        o.push_back(0);
        if (! index_impl(inner, id, o))
          return false;
        o.pop_back();
      }
    }
    else
    {
      bitmap_index* idx;
      auto i = indexes_.find(o);
      if (i != indexes_.end())
      {
        idx = i->second.get();
      }
      else
      {
        auto bmi = bitmap_index::create(v.which());
        idx = bmi.get();
        indexes_.emplace(o, std::move(bmi));
      }
      assert(idx != nullptr);
      if (! append_value(*idx, id, v))
        return false;
      ++o.back();
    }
  }

  return true;
}

} // namespace vast
