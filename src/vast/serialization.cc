#include "vast/serialization.h"

#include <caf/all.hpp>
#include "vast/actor.h"
#include "vast/bitmap_index.h"
#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/expression.h"
#include "vast/event.h"
#include "vast/error.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/value.h"
#include "vast/type.h"
#include "vast/uuid.h"
#include "vast/serialization/all.h"
#include "vast/detail/caf_type_info.h"
#include "vast/detail/type_manager.h"
#include "vast/util/coding.h"
#include "vast/util/meta.h"
#include "vast/util/pp.h"
#include "vast/util/tuple.h"

namespace vast {

bool serializer::begin_instance(std::type_info const& /* ti */)
{
  // Do nothing by default.
  VAST_ENTER();
  //if (global_typeid(ti) == nullptr)
  //{
  //  VAST_LOG_ERROR("missing type info for " << detail::demangle(ti));
  //  VAST_RETURN(false);
  //}
  VAST_RETURN(true);
}

bool serializer::end_instance()
{
  // Do nothing by default.
  VAST_ENTER();
  VAST_RETURN(true);
}

bool serializer::end_sequence()
{
  // Do nothing by default.
  VAST_ENTER();
  VAST_RETURN(true);
}

bool serializer::write_string(char const* data, size_t size)
{
  VAST_ENTER(VAST_ARG(data, size));
  VAST_RETURN(write_raw(data, size));
}

bool serializer::write_type(global_type_info const* gti)
{
  VAST_ENTER();
  assert(gti != nullptr);
  access::serializable::save(*this, gti->id());
  VAST_RETURN(true);
}

bool deserializer::begin_instance(std::type_info const& /* ti */)
{
  VAST_ENTER();
  //if (global_typeid(ti) == nullptr)
  //{
  //  VAST_LOG_ERROR("missing type info for " << detail::demangle(ti));
  //  VAST_RETURN(false);
  //}
  VAST_RETURN(true);
}

bool deserializer::end_instance()
{
  // Do nothing by default.
  VAST_ENTER();
  VAST_RETURN(true);
}

bool deserializer::end_sequence()
{
  // Do nothing by default.
  VAST_ENTER();
  VAST_RETURN(true);
}

bool deserializer::read_string(char* data, size_t size)
{
  VAST_ENTER();
  VAST_RETURN(read_raw(data, size));
}

bool deserializer::read_type(global_type_info const*& gti)
{
  VAST_ENTER();

  type_id id = 0;
  access::serializable::load(*this, id);

  gti = global_typeid(id);
  if (gti)
    VAST_RETURN(true);

  VAST_LOG_ERROR("no type info for id " << id);
  VAST_RETURN(false);
}

binary_serializer::binary_serializer(io::output_stream& sink)
  : sink_(sink)
{
}

bool binary_serializer::begin_sequence(uint64_t size)
{
  VAST_ENTER();
  bytes_ += util::varbyte::size(size);
  VAST_RETURN(sink_.write_varbyte(&size));
}

bool binary_serializer::write_bool(bool x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(bool);
  VAST_RETURN(sink_.write<uint8_t>(&x));
}

bool binary_serializer::write_int8(int8_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(int8_t);
  VAST_RETURN(sink_.write<int8_t>(&x));
}

bool binary_serializer::write_uint8(uint8_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(uint8_t);
  VAST_RETURN(sink_.write<uint8_t>(&x));
}

bool binary_serializer::write_int16(int16_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(int16_t);
  VAST_RETURN(sink_.write<int16_t>(&x));
}

bool binary_serializer::write_uint16(uint16_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(uint16_t);
  VAST_RETURN(sink_.write<uint16_t>(&x));
}

bool binary_serializer::write_int32(int32_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(int32_t);
  VAST_RETURN(sink_.write<int32_t>(&x));
}

bool binary_serializer::write_uint32(uint32_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(uint32_t);
  VAST_RETURN(sink_.write<uint32_t>(&x));
}

bool binary_serializer::write_int64(int64_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(int64_t);
  VAST_RETURN(sink_.write<int64_t>(&x));
}

bool binary_serializer::write_uint64(uint64_t x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(uint64_t);
  VAST_RETURN(sink_.write<uint64_t>(&x));
}

bool binary_serializer::write_double(double x)
{
  VAST_ENTER(VAST_ARG(x));
  bytes_ += sizeof(double);
  VAST_RETURN(sink_.write<double>(&x));
}

bool binary_serializer::write_raw(void const* data, size_t size)
{
  VAST_ENTER(VAST_ARG(data, size));
  bytes_ += size;
  VAST_RETURN(sink_.write_raw(data, size));
}

size_t binary_serializer::bytes() const
{
  return bytes_;
}


binary_deserializer::binary_deserializer(io::input_stream& source)
  : source_(source)
{
}

bool binary_deserializer::begin_sequence(uint64_t& size)
{
  VAST_ENTER();
  auto success = source_.read_varbyte(&size);
  bytes_ += util::varbyte::size(size);
  VAST_RETURN(success);
}

bool binary_deserializer::read_bool(bool& x)
{
  VAST_ENTER();
  bytes_ += sizeof(bool);
  VAST_RETURN(source_.read<uint8_t>(&x), x);
}

bool binary_deserializer::read_int8(int8_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(int8_t);
  VAST_RETURN(source_.read<int8_t>(&x), x);
}

bool binary_deserializer::read_uint8(uint8_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(uint8_t);
  VAST_RETURN(source_.read<uint8_t>(&x), x);
}

bool binary_deserializer::read_int16(int16_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(int16_t);
  VAST_RETURN(source_.read<int16_t>(&x), x);
}

bool binary_deserializer::read_uint16(uint16_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(uint16_t);
  VAST_RETURN(source_.read<uint16_t>(&x), x);
}

bool binary_deserializer::read_int32(int32_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(int32_t);
  VAST_RETURN(source_.read<int32_t>(&x), x);
}

bool binary_deserializer::read_uint32(uint32_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(uint32_t);
  VAST_RETURN(source_.read<uint32_t>(&x), x);
}

bool binary_deserializer::read_int64(int64_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(int64_t);
  VAST_RETURN(source_.read<int64_t>(&x), x);
}

bool binary_deserializer::read_uint64(uint64_t& x)
{
  VAST_ENTER();
  bytes_ += sizeof(uint64_t);
  VAST_RETURN(source_.read<uint64_t>(&x), x);
}

bool binary_deserializer::read_double(double& x)
{
  VAST_ENTER();
  bytes_ += sizeof(double);
  VAST_RETURN(source_.read<double>(&x), x);
}

bool binary_deserializer::read_raw(void* data, size_t size)
{
  VAST_ENTER();
  bytes_ += size;
  VAST_RETURN(source_.read_raw(data, size));
}

size_t binary_deserializer::bytes() const
{
  return bytes_;
}

type_id global_type_info::id() const
{
  return id_;
}

object global_type_info::create() const
{
  return {this, construct()};
}

global_type_info::global_type_info(type_id id)
  : id_(id)
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

bool is_convertible(global_type_info const* from, std::type_info const& to)
{
  return detail::type_manager::instance()->check_link(from, to);
}

namespace {

template <template <typename> class F, typename L>
auto apply()
{
  using tuple =
    typename util::tl_apply<
      typename util::tl_map<L, F>::type,
      std::tuple
    >::type;

  util::static_for_each([](auto f) { f(); }, tuple{});
}

template <typename From, typename To>
struct converter
{
  using type = converter;

  void operator()() const
  {
    make_convertible<From, To>();
  }
};

template <typename T>
using bs_converter = converter<T, detail::bitstream_concept>;

template <typename From>
struct bmi_converter
{
  using type = bmi_converter;

  void operator()() const
  {
    make_convertible<
      detail::bitmap_index_model<From>,
      detail::bitmap_index_concept<typename From::bitstream_type>
    >();
  }
};

template <typename T>
auto caf_announce(std::string name)
{
  // VAST
  announce<T>();

  // CAF
  auto ti = std::make_unique<detail::caf_type_info<T>>(std::move(name));
  return caf::announce(typeid(T), std::move(ti));
}

// Needed only because the macro below cannot handle the commata.
template <typename T>
using abi_null = arithmetic_bitmap_index<null_bitstream, T>;

template <typename T>
using abi_ewah = arithmetic_bitmap_index<ewah_bitstream, T>;

} // namespace <anonymous>

#define VAST_ANNOUNCE2(T, name) caf_announce<T>(name)
#define VAST_ANNOUNCE1(T)       VAST_ANNOUNCE2(T, #T)
#define VAST_ANNOUNCE(...)      VAST_PP_OVERLOAD(VAST_ANNOUNCE, \
                                                 __VA_ARGS__)(__VA_ARGS__)
void announce_builtin_types()
{
  // Core
  VAST_ANNOUNCE(address);
  VAST_ANNOUNCE(arithmetic_operator);
  VAST_ANNOUNCE(block);
  VAST_ANNOUNCE(boolean_operator);
  VAST_ANNOUNCE(chunk);
  VAST_ANNOUNCE(data);
  VAST_ANNOUNCE(error);
  VAST_ANNOUNCE(expression);
  VAST_ANNOUNCE(event);
  VAST_ANNOUNCE(key);
  VAST_ANNOUNCE(none);
  VAST_ANNOUNCE(offset);
  VAST_ANNOUNCE(path);
  VAST_ANNOUNCE(pattern);
  VAST_ANNOUNCE(port);
  VAST_ANNOUNCE(record);
  VAST_ANNOUNCE(relational_operator);
  VAST_ANNOUNCE(schema);
  VAST_ANNOUNCE(set);
  VAST_ANNOUNCE(subnet);
  VAST_ANNOUNCE(table);
  VAST_ANNOUNCE(time_point);
  VAST_ANNOUNCE(time_duration);
  VAST_ANNOUNCE(type);
  VAST_ANNOUNCE(uuid);
  VAST_ANNOUNCE(value);
  VAST_ANNOUNCE(vector);
  // std::vector<T>
  VAST_ANNOUNCE(std::vector<data>);
  VAST_ANNOUNCE(std::vector<event>);
  VAST_ANNOUNCE(std::vector<value>);
  VAST_ANNOUNCE(std::vector<uuid>);
  // Bitstream
  VAST_ANNOUNCE(bitstream);
  VAST_ANNOUNCE(detail::bitstream_model<ewah_bitstream>);
  VAST_ANNOUNCE(detail::bitstream_model<null_bitstream>);
  // Bitmap Index
  VAST_ANNOUNCE(bitmap_index<ewah_bitstream>);
  VAST_ANNOUNCE(bitmap_index<null_bitstream>);
  VAST_ANNOUNCE(abi_null<boolean>);
  VAST_ANNOUNCE(abi_null<integer>);
  VAST_ANNOUNCE(abi_null<count>);
  VAST_ANNOUNCE(abi_null<real>);
  VAST_ANNOUNCE(abi_null<time_point>);
  VAST_ANNOUNCE(abi_null<time_duration>);
  VAST_ANNOUNCE(abi_ewah<boolean>);
  VAST_ANNOUNCE(abi_ewah<integer>);
  VAST_ANNOUNCE(abi_ewah<count>);
  VAST_ANNOUNCE(abi_ewah<real>);
  VAST_ANNOUNCE(abi_ewah<time_point>);
  VAST_ANNOUNCE(abi_ewah<time_duration>);
  VAST_ANNOUNCE(address_bitmap_index<null_bitstream>);
  VAST_ANNOUNCE(subnet_bitmap_index<null_bitstream>);
  VAST_ANNOUNCE(port_bitmap_index<null_bitstream>);
  VAST_ANNOUNCE(string_bitmap_index<null_bitstream>);
  VAST_ANNOUNCE(sequence_bitmap_index<null_bitstream>);
  VAST_ANNOUNCE(address_bitmap_index<ewah_bitstream>);
  VAST_ANNOUNCE(subnet_bitmap_index<ewah_bitstream>);
  VAST_ANNOUNCE(port_bitmap_index<ewah_bitstream>);
  VAST_ANNOUNCE(string_bitmap_index<ewah_bitstream>);
  VAST_ANNOUNCE(sequence_bitmap_index<ewah_bitstream>);
  VAST_ANNOUNCE(detail::bitmap_index_model<address_bitmap_index<null_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<subnet_bitmap_index<null_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<port_bitmap_index<null_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<string_bitmap_index<null_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<sequence_bitmap_index<null_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<address_bitmap_index<ewah_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<subnet_bitmap_index<ewah_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<port_bitmap_index<ewah_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<string_bitmap_index<ewah_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<sequence_bitmap_index<ewah_bitstream>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_null<boolean>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_null<integer>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_null<count>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_null<real>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_null<time_point>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_null<time_duration>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_ewah<boolean>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_ewah<integer>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_ewah<count>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_ewah<real>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_ewah<time_point>>);
  VAST_ANNOUNCE(detail::bitmap_index_model<abi_ewah<time_duration>>);


  caf::announce<std::vector<caf::actor>>("std::vector<actor>");
  caf::announce<flow_control::announce>("flow_control::annoucne",
                                        &flow_control::announce::source);
  caf::announce<flow_control::overload>("flow_control::overload");
  caf::announce<flow_control::underload>("flow_control::underload");

  // Polymorphic types
  using bitstream_models = util::type_list<
    detail::bitstream_model<ewah_bitstream>,
    detail::bitstream_model<null_bitstream>
  >;

  using bmi_types = util::type_list<
    arithmetic_bitmap_index<null_bitstream, boolean>,
    arithmetic_bitmap_index<null_bitstream, integer>,
    arithmetic_bitmap_index<null_bitstream, count>,
    arithmetic_bitmap_index<null_bitstream, real>,
    arithmetic_bitmap_index<null_bitstream, time_point>,
    arithmetic_bitmap_index<null_bitstream, time_duration>,
    address_bitmap_index<null_bitstream>,
    subnet_bitmap_index<null_bitstream>,
    port_bitmap_index<null_bitstream>,
    string_bitmap_index<null_bitstream>,
    sequence_bitmap_index<null_bitstream>,
    arithmetic_bitmap_index<ewah_bitstream, boolean>,
    arithmetic_bitmap_index<ewah_bitstream, integer>,
    arithmetic_bitmap_index<ewah_bitstream, count>,
    arithmetic_bitmap_index<ewah_bitstream, real>,
    arithmetic_bitmap_index<ewah_bitstream, time_point>,
    arithmetic_bitmap_index<ewah_bitstream, time_duration>,
    address_bitmap_index<ewah_bitstream>,
    subnet_bitmap_index<ewah_bitstream>,
    port_bitmap_index<ewah_bitstream>,
    string_bitmap_index<ewah_bitstream>,
    sequence_bitmap_index<ewah_bitstream>
  >;

  apply<bs_converter, bitstream_models>();
  apply<bmi_converter, bmi_types>();
}

#undef VAST_ANNOUNCE
#undef VAST_ANNOUNCE1
#undef VAST_ANNOUNCE2

object::object(global_type_info const* type, void* value)
  : type_(type), value_(value)
{
  assert(type_ != nullptr);
  assert(value_ != nullptr);
}

object::object(object const& other)
{
  if (other)
  {
    type_ = other.type_;
    value_ = type_->construct(other.value_);
  }
}

object::object(object&& other) noexcept
  : type_(other.type_), value_(other.value_)
{
  other.type_ = nullptr;
  other.value_ = nullptr;
}

object& object::operator=(object other)
{
  std::swap(type_, other.type_);
  std::swap(value_, other.value_);
  return *this;
}

object::~object()
{
  if (*this)
    type_->destruct(value_);
}

object::operator bool() const
{
  return value_ != nullptr && type_ != nullptr;
}

bool operator==(object const& x, object const& y)
{
  return x.type() == y.type()
    ? (x.value() == y.value() || x.type()->equals(x.value(), y.value()))
    : false;
}

global_type_info const* object::type() const
{
  return type_;
}

void const* object::value() const
{
  return value_;
}

void* object::value()
{
  return value_;
}

void* object::release()
{
  auto ptr = value_;
  type_ = nullptr;
  value_ = nullptr;
  return ptr;
}

void object::serialize(serializer& sink) const
{
  VAST_ENTER();
  assert(*this);
  sink.write_type(type_);
  type_->serialize(sink, value_);
}

void object::deserialize(deserializer& source)
{
  VAST_ENTER();
  if (*this)
    type_->destruct(value_);

  if (! source.read_type(type_) )
  {
    VAST_LOG_ERROR("failed to deserialize object type");
  }
  else if (type_ == nullptr)
  {
    VAST_LOG_ERROR("deserialized an invalid object type");
  }
  else
  {
    value_ = type_->construct();
    type_->deserialize(source, value_);
  }
}

} // namespace vast
