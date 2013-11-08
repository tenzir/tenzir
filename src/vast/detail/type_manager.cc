#include "vast/detail/type_manager.h"

#include <cassert>
#include <exception>
#include "vast/bitstream.h"
#include "vast/container.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/type_info.h"
#include "vast/event.h"
#include "vast/value.h"
#include "vast/bitmap_index/arithmetic.h"
#include "vast/bitmap_index/address.h"
#include "vast/bitmap_index/port.h"
#include "vast/bitmap_index/string.h"
#include "vast/bitmap_index/time.h"
#include "vast/detail/cppa_type_info.h"
#include "vast/util/tuple.h"

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

  VAST_LOG_DEBUG("registering new type " << detail::demangle(ti.name()) <<
                 " with id " << id_ << " (mangled name: " << ti.name() << ")");

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
  if (! from)
    return false;
  if (*from == to)
    return false; // We do not store reflexivity...

  auto& set = conversions_[from->id()];
  auto t = set.find(std::type_index(to));
  if (t != set.end())
  {
    VAST_LOG_WARN("attempted to register duplicate conversion from type " <<
                  from->name() << " to type " << detail::demangle(to));
    return false;
  }
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


// TODO: Use polymorphic lambdas in C++14.
namespace {

struct type_announcer
{
  template <typename T>
  void operator()(T /* x */) const
  {
    announce<T>();
  }
};

template <typename To>
struct type_converter
{
  template <typename T>
  void operator()(T /* x */) const
  {
    make_convertible<T, To>();
  }
};

} // namespace <anonymous>

void type_manager::initialize()
{
  std::tuple<
    bool, double,
    int8_t, int16_t, int32_t, int64_t,
    uint8_t, uint16_t, uint32_t, uint64_t
  > integral_types;

  std::tuple<
    std::string,
    std::vector<std::string>
  > stl_types;

  std::tuple<
    address,
    time_range,
    time_point,
    port,
    prefix,
    regex,
    string,
    record,
    table,
    value_type,
    value, std::vector<value>,
    event, std::vector<event>,
    path,
    expr::ast
  > vast_types;

  std::tuple<
    expr::constant,
    expr::timestamp_extractor,
    expr::name_extractor,
    expr::id_extractor,
    expr::offset_extractor,
    expr::type_extractor,
    expr::predicate,
    expr::conjunction,
    expr::disjunction
  > expr_node_types;

  std::tuple<
    detail::bitstream_model<ewah_bitstream>,
    detail::bitstream_model<null_bitstream>
  > bitstream_types;

  std::tuple<
    arithmetic_bitmap_index<bool_type>,
    arithmetic_bitmap_index<int_type>,
    arithmetic_bitmap_index<uint_type>,
    arithmetic_bitmap_index<double_type>,
    address_bitmap_index,
    port_bitmap_index,
    time_bitmap_index,
    string_bitmap_index
  > bitmap_index_types;

  util::for_each(integral_types, type_announcer{});
  util::for_each(stl_types, type_announcer{});
  util::for_each(vast_types, type_announcer{});
  util::for_each(expr_node_types, type_announcer{});
  util::for_each(bitstream_types, type_announcer{});
  util::for_each(bitmap_index_types, type_announcer{});

  util::for_each(expr_node_types, type_converter<expr::node>{});
  util::for_each(bitmap_index_types, type_converter<bitmap_index>{});
  util::for_each(bitstream_types, type_converter<detail::bitstream_concept>{});

  cppa_announce_types();
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
