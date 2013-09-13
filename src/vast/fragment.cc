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

bool fragment::append(bitmap_index& bmi, uint64_t id, value const& val)
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
  VAST_LOG_ACT_DEBUG("meta-fragment", "loads indexes from disk");
  io::unarchive(dir_ / "timestamp.idx", timestamp_);
  io::unarchive(dir_ / "name.idx", name_);
}

void meta_fragment::store()
{
  VAST_LOG_ACT_DEBUG("meta-fragment", "writes indexes to disk");
  io::archive(dir_ / "timestamp.idx", timestamp_);
  io::archive(dir_ / "name.idx", name_);
}

void meta_fragment::index(event const& e)
{
  if (! append(timestamp_, e.id(), e.timestamp()))
  {
    VAST_LOG_ACT_ERROR("fragment",
                       "failed to index event timestamp " << e.timestamp());
    quit();
  }

  if (! append(name_, e.id(), e.name()))
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
  io::unarchive(dir_ / "bool.idx", bool_);
  io::unarchive(dir_ / "int.idx", int_);
  io::unarchive(dir_ / "uint.idx", uint_);
  io::unarchive(dir_ / "double.idx", double_);
  io::unarchive(dir_ / "time-range.idx", time_range_);
  io::unarchive(dir_ / "time-point.idx", time_point_);
  io::unarchive(dir_ / "string.idx", string_);
  io::unarchive(dir_ / "address.idx", address_);
  io::unarchive(dir_ / "port.idx", port_);
}

void type_fragment::store()
{
  io::archive(dir_ / "bool.idx", bool_);
  io::archive(dir_ / "int.idx", int_);
  io::archive(dir_ / "uint.idx", uint_);
  io::archive(dir_ / "double.idx", double_);
  io::archive(dir_ / "time-range.idx", time_range_);
  io::archive(dir_ / "time-point.idx", time_point_);
  io::archive(dir_ / "string.idx", string_);
  io::archive(dir_ / "address.idx", address_);
  io::archive(dir_ / "port.idx", port_);
}

void type_fragment::index(event const& e)
{
  std::set<value> unique;
  for (auto& v : e)
    if (! unique.count(v))
    {
      index_impl(e.id(), v); // TODO: Handle error case.
      unique.insert(v);
    }
}

bool type_fragment::index_impl(uint64_t id, const value& v)
{
  switch (v.which())
  {
    default:
      VAST_LOG_ACT_ERROR("fragment", "cannot handle value type " << v.which());
      break;
    case bool_type:
      return append(bool_, id, v);
    case int_type:
      return append(int_, id, v);
    case uint_type:
      return append(uint_, id, v);
    case double_type:
      return append(double_, id, v);
    case time_range_type:
      return append(time_range_, id, v);
    case time_point_type:
      return append(time_point_, id, v);
    case string_type:
      return append(string_, id, v);
    case address_type:
      return append(address_, id, v);
    case port_type:
      return append(port_, id, v);
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
  std::set<path> paths;
  traverse(dir_, [&](path const& p) -> bool { paths.insert(p); return true; });

  for (auto& p : paths)
  {
    std::unique_ptr<bitmap_index> bi;
    io::unarchive(p, bi);
    offset o;
    // TODO: convert path into an offset.
    indexes_.emplace(o, std::move(bi));
  }
}

void argument_fragment::store()
{
  static string prefix{"@"};
  static string suffix{".idx"};
  for (auto& p : indexes_)
  {
    // TODO: convert offset into a path.
    path const filename = dir_ / (prefix + "p.first" + suffix);
    io::archive(filename, p.second);
  }
}

void argument_fragment::index(event const& e)
{
  if (e.empty())
    return;
  offset o{0};
  index_impl(e, e.id(), o); // TODO: Handle error case.
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
    if (v.which() == record_type)
    {
      auto& inner = v.get<record>();
      if (! inner.empty())
      {
        o.push_back(0);
        index_impl(inner, id, o);
        o.pop_back();
      }
    }
    else
    {
      auto idx = indexes_[o].get();
      if (! idx)
      {
        auto bi = bitmap_index::create(v.which());
        idx = bi.get();
        indexes_.emplace(o, std::move(bi));
        assert(idx);
      }
      if (! append(*idx, id, v))
        return false;
      ++o.back();
    }
  }

  return true;
}

} // namespace vast
