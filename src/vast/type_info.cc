#include "vast/type_info.h"

#include "vast/object.h"
#include "vast/serialization.h"
#include "vast/detail/type_manager.h"

namespace vast {

type_id global_type_info::id() const
{
  return id_;
}

std::string const& global_type_info::name() const
{
  return name_;
}

object global_type_info::create() const
{
  return {construct(), this};
}

global_type_info::global_type_info(type_id id, std::string name)
  : id_(id), name_(std::move(name))
{
}

bool operator<(global_type_info const& x, global_type_info const& y)
{
  return x.id_ < y.id_;
}

bool operator==(global_type_info const& x, global_type_info const& y)
{
  return &x == &y;
}

bool operator==(global_type_info const& x, std::type_info const& y)
{
  return x.equals(y);
}

namespace detail {
bool register_type(std::type_info const& ti,
                   std::function<global_type_info*(type_id)> f)
{
  return detail::type_manager::instance()->add(ti, f);
}
} // namespace detail

global_type_info const* global_typeid(std::type_info const& ti)
{
  return detail::type_manager::instance()->lookup(ti);
}

global_type_info const* global_typeid(type_id id)
{
  return detail::type_manager::instance()->lookup(id);
}

global_type_info const* global_typeid(std::string const& name)
{
  return detail::type_manager::instance()->lookup(name);
}

} // namespace vast

