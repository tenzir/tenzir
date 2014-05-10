#include "vast/type_info.h"

#include <cppa/cppa.hpp>
#include "vast/bitstream.h"
#include "vast/cow.h"
#include "vast/expression.h"
#include "vast/event.h"
#include "vast/error.h"
#include "vast/file_system.h"
#include "vast/object.h"
#include "vast/value.h"
#include "vast/search_result.h"
#include "vast/segment.h"
#include "vast/type.h"
#include "vast/serialization.h"
#include "vast/bitmap_index.h"
#include "vast/detail/cppa_type_info.h"
#include "vast/detail/type_manager.h"
#include "vast/util/tuple.h"

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
  return {this, construct()};
}

global_type_info::global_type_info(type_id id, std::string name)
  : id_(id), name_(std::move(name))
{
}

bool operator==(global_type_info const& x, global_type_info const& y)
{
  return &x == &y;
}

bool operator==(global_type_info const& x, std::type_info const& y)
{
  return x.equals(y);
}

bool operator!=(global_type_info const& x, std::type_info const& y)
{
  return ! (x == y);
}

bool operator<(global_type_info const& x, global_type_info const& y)
{
  return x.id_ < y.id_;
}

namespace detail {

bool register_type(std::type_info const& ti,
                   std::function<global_type_info*(type_id)> f)
{
  return detail::type_manager::instance()->add(ti, f);
}

bool add_link(global_type_info const* from, std::type_info const& to)
{
  return detail::type_manager::instance()->add_link(from, to);
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

bool is_convertible(global_type_info const* from, std::type_info const& to)
{
  return detail::type_manager::instance()->check_link(from, to);
}

namespace {
// TODO: Use polymorphic lambdas in C++14.

struct type_announcer
{
  template <typename T>
  void operator()(T /* x */) const
  {
    announce<T>();
    cppa::announce(typeid(T), make_unique<detail::cppa_type_info<T>>());
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

void announce_builtin_types()
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
    invalid_type,
    bool_type,
    int_type,
    uint_type,
    double_type,
    time_range_type,
    time_point_type,
    string_type,
    regex_type,
    address_type,
    prefix_type,
    port_type,
    enum_type,
    vector_type,
    set_type,
    table_type,
    argument,
    record_type,
    type,

    address,
    time_range,
    time_point,
    port,
    prefix,
    regex,
    string, std::vector<string>,
    record,
    table,
    type_tag,
    value, std::vector<value>,
    event, std::vector<event>, std::vector<cow<event>>,
    error,

    chunk,
    offset,
    key,
    path,
    segment,
    uuid, std::vector<uuid>,

    arithmetic_operator, boolean_operator, relational_operator,
    bitstream,
    expr::ast,
    schema,
    search_result
  > vast_types;

  std::tuple<
    expr::constant,
    expr::timestamp_extractor,
    expr::name_extractor,
    expr::id_extractor,
    expr::offset_extractor,
    expr::schema_extractor,
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
    arithmetic_bitmap_index<null_bitstream, bool_value>,
    arithmetic_bitmap_index<null_bitstream, int_value>,
    arithmetic_bitmap_index<null_bitstream, uint_value>,
    arithmetic_bitmap_index<null_bitstream, double_value>,
    arithmetic_bitmap_index<null_bitstream, time_range_value>,
    arithmetic_bitmap_index<null_bitstream, time_point_value>,
    address_bitmap_index<null_bitstream>,
    port_bitmap_index<null_bitstream>,
    string_bitmap_index<null_bitstream>,
    arithmetic_bitmap_index<ewah_bitstream, bool_value>,
    arithmetic_bitmap_index<ewah_bitstream, int_value>,
    arithmetic_bitmap_index<ewah_bitstream, uint_value>,
    arithmetic_bitmap_index<ewah_bitstream, double_value>,
    arithmetic_bitmap_index<ewah_bitstream, time_range_value>,
    arithmetic_bitmap_index<ewah_bitstream, time_point_value>,
    address_bitmap_index<ewah_bitstream>,
    port_bitmap_index<ewah_bitstream>,
    string_bitmap_index<ewah_bitstream>
  > bitmap_index_types;

  util::for_each(integral_types, type_announcer{});
  util::for_each(stl_types, type_announcer{});
  util::for_each(vast_types, type_announcer{});
  util::for_each(expr_node_types, type_announcer{});
  util::for_each(bitstream_types, type_announcer{});
  util::for_each(bitmap_index_types, type_announcer{});

  util::for_each(expr_node_types, type_converter<expr::node>{});
  util::for_each(bitstream_types, type_converter<detail::bitstream_concept>{});
}

} // namespace vast
