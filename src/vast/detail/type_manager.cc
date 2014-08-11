#include "vast/detail/type_manager.h"

#include <cassert>
#include <exception>
#include "vast/serialization.h"

namespace vast {
namespace detail {

bool
type_manager::add(std::type_info const& ti,
                  std::function<global_type_info*(type_id)> f)
{
  if (by_ti_.find(std::type_index(ti)) != by_ti_.end())
    return false;
  if (by_name_.find(ti.name()) != by_name_.end())
    return false;;

  auto gti = f(++id_);

  by_id_.emplace(gti->id(), gti);
  by_name_.emplace(gti->name(), gti);
  by_ti_.emplace(std::type_index(ti), std::unique_ptr<global_type_info>(gti));

  return true;
}

global_type_info const* type_manager::lookup(std::type_info const& ti) const
{
  auto i = by_ti_.find(std::type_index(ti));
  if (i == by_ti_.end())
    return nullptr;
  return i->second.get();
}

global_type_info const* type_manager::lookup(type_id id) const
{
  auto i = by_id_.find(id);
  if (i == by_id_.end())
    return nullptr;
  return i->second;
}

global_type_info const* type_manager::lookup(std::string const& name) const
{
  auto i = by_name_.find(name);
  if (i == by_name_.end())
    return nullptr;
  return i->second;
}

// TODO: automatically build transitive closure over linked types.
bool type_manager::add_link(global_type_info const* from,
                            std::type_info const& to)
{
  if (! from || *from == to)
    return false;

  auto& set = conversions_[from->id()];
  auto t = set.find(std::type_index(to));
  if (t != set.end())
    return false;

  set.emplace(to);
  return true;
}

bool type_manager::check_link(global_type_info const* from,
                              std::type_info const& to) const
{
  if (! from)
    return false;
  if (*from == to)
    return true; // ...but acknowledge it nonetheless.

  auto s = conversions_.find(from->id());
  if (s == conversions_.end())
    return false;
  return s->second.count(std::type_index(to));
}

namespace {
struct gti_key_cmp
{
  bool operator()(global_type_info const* x, global_type_info const* y) const
  {
    return x->id() < y->id();
  };
};
} // namespace <anonymous>

void type_manager::each(std::function<void(global_type_info const&)> f) const
{
  std::set<global_type_info const*, gti_key_cmp> sorted;
  for (auto& p : by_ti_)
    sorted.insert(p.second.get());
  for (auto gti : sorted)
    f(*gti);
}

type_manager* type_manager::create()
{
  return new type_manager;
}

void type_manager::initialize()
{
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
