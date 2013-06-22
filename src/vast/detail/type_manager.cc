#include "vast/detail/type_manager.h"

#include <exception>
#include "vast/type_info.h"

namespace vast {
namespace detail {

void type_manager::add(std::type_info const& ti,
                       std::unique_ptr<stable_type_info> uti)
{
  if (by_id_.find(uti->id()) != by_id_.end())
    throw std::logic_error("duplicate type ID");

  if (by_name_.find(uti->name()) != by_name_.end())
    throw std::logic_error("duplicate type name");

  auto idx = std::type_index(ti);
  if (by_ti_.find(idx) != by_ti_.end())
    throw std::logic_error("duplicate std::type_info");

  by_id_.emplace(uti->id(), uti.get());
  by_name_.emplace(uti->name(), uti.get());
  by_ti_.emplace(std::move(idx), std::move(uti));
}

stable_type_info const* type_manager::lookup(std::type_info const& ti) const
{
  auto i = by_ti_.find(std::type_index(ti));
  if (i == by_ti_.end())
    return nullptr;
  return i->second.get();
}

stable_type_info const* type_manager::lookup(type_id id) const
{
  auto i = by_id_.find(id);
  if (i == by_id_.end())
    return nullptr;
  return i->second;
}

stable_type_info const* type_manager::lookup(std::string const& name) const
{
  auto i = by_name_.find(name);
  if (i == by_name_.end())
    return nullptr;
  return i->second;
}

type_manager* type_manager::create()
{
  return new type_manager;
}

void type_manager::initialize()
{
  // TODO: announce built-in types.
}

void type_manager::destroy()
{
  delete this;
}

void type_manager::dispose()
{
  delete this;
}

} // namespace detail
} // namespace vast
