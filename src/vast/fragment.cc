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
  // TODO: traverse the directory and load existing indexes.
  VAST_LOG_VERBOSE(VAST_ACTOR("fragment") << "spawned");

  become(
      on(atom("load")) >> [=]
      {
        if (exists(dir_))
          load(dir_);
      },
      on(atom("store")) >> [=]
      {
        store(dir_);
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
      },
      on(atom("kill")) >> [=]
      {
        store(dir_);
        quit();
      });
}

void fragment::on_exit()
{
  VAST_LOG_VERBOSE(VAST_ACTOR("fragment") << " terminated");
}

bool fragment::append(bitmap_index& bmi, uint64_t event_id, value const& val)
{
  if (event_id < bmi.size())
  {
    VAST_LOG_ERROR(VAST_ACTOR("fragment") <<
                   " encoutered event with incompatible ID: " << event_id);
    return false;
  }

  auto delta = event_id - bmi.size();
  if (delta > 1)
    if (! bmi.append(delta - 1, false))
      return false;

  return bmi.push_back(val);
}

meta_fragment::meta_fragment(path dir)
  : fragment(std::move(dir))
{
}

void meta_fragment::load(path const& p)
{
  io::archive(p / "timestamp.idx", timestamp_);
  io::archive(p / "name.idx", name_);
}

void meta_fragment::store(path const& p)
{
  io::unarchive(p / "timestamp.idx", timestamp_);
  io::unarchive(p / "name.idx", name_);
}

void meta_fragment::index(const event& e)
{
  if (! append(timestamp_, e.id(), e.timestamp()))
  {
    VAST_LOG_ERROR(VAST_ACTOR("fragment") << 
                   " failed to index event timestamp " << e.timestamp());
    quit();
  }

  if (! append(name_, e.id(), e.name()))
  {
    VAST_LOG_ERROR(VAST_ACTOR("fragment") << 
                   " failed to index event name " << e.name());
    quit();
  }
}

option<bitstream> meta_fragment::lookup(expression const& e)
{
  // TODO.
  return {};
}


type_fragment::type_fragment(path dir)
  : fragment(std::move(dir))
{
}

void type_fragment::load(path const& p)
{
  io::archive(p / "bool.idx", bool_);
  io::archive(p / "int.idx", int_);
  io::archive(p / "uint.idx", uint_);
  io::archive(p / "double.idx", double_);
  io::archive(p / "time-range.idx", time_range_);
  io::archive(p / "time-point.idx", time_point_);
  io::archive(p / "string.idx", string_);
  io::archive(p / "address.idx", address_);
  io::archive(p / "port.idx", port_);
}

void type_fragment::store(path const& p)
{
  io::unarchive(p / "bool.idx", bool_);
  io::unarchive(p / "int.idx", int_);
  io::unarchive(p / "uint.idx", uint_);
  io::unarchive(p / "double.idx", double_);
  io::unarchive(p / "time-range.idx", time_range_);
  io::unarchive(p / "time-point.idx", time_point_);
  io::unarchive(p / "string.idx", string_);
  io::unarchive(p / "address.idx", address_);
  io::unarchive(p / "port.idx", port_);
}

void type_fragment::index(const event& e)
{
  for (auto& v : e)
    index(e.id(), v);
}

bool type_fragment::index(uint64_t event_id, const value& v)
{
  switch (v.which())
  {
    default:
      VAST_LOG_ERROR(VAST_ACTOR("fragment") <<
                     " cannot index a value of type " << v.which());
      break;
    case bool_type:
      return append(bool_, event_id, v);
    case int_type:
      return append(int_, event_id, v);
    case uint_type:
      return append(uint_, event_id, v);
    case double_type:
      return append(double_, event_id, v);
    case time_range_type:
      return append(time_range_, event_id, v);
    case time_point_type:
      return append(time_point_, event_id, v);
    case string_type:
      return append(string_, event_id, v);
    case address_type:
      return append(address_, event_id, v);
    case port_type:
      return append(port_, event_id, v);
    case record_type:
      for (auto& rv : v.get<record>())
        if (! index(event_id, rv))
          return false;
  }
  return true;
}

option<bitstream> type_fragment::lookup(expression const& e)
{
  // TODO.
  return {};
}

} // namespace vast
