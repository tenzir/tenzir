#include "vast/type_info.h"

#include <cassert>
#include "vast/detail/type_manager.h"

namespace vast {

type_id stable_type_info::id() const
{
  return id_;
}

std::string const& stable_type_info::name() const
{
  return name_;
}

stable_type_info::stable_type_info(type_id id, std::string name)
  : id_(id), name_(std::move(name))
{
  assert(id_ != 0);
}

stable_type_info const* stable_typeid(std::type_info const& ti)
{
  return detail::type_manager::instance()->lookup(ti);
}

bool operator<(stable_type_info const& x, stable_type_info const& y)
{
  return x.id_ < y.id_;
}

bool operator==(stable_type_info const& x, stable_type_info const& y)
{
  return &x == &y;
}

bool operator==(stable_type_info const& x, std::type_info const& y)
{
  return x.equals(y);
}

} // namespace vast

