#include "vast/serialization.h"

#include "vast/logger.h"
#include "vast/type_info.h"
#include "vast/detail/type_manager.h"
#include "vast/util/coding.h"

namespace vast {

bool serializer::typed() const
{
  return true;
}

bool serializer::begin_object(global_type_info const& ti)
{
  VAST_ENTER();
  save(*this, ti.id());
  VAST_RETURN(true);
}

bool serializer::end_object()
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

bool deserializer::typed() const
{
  return true;
}

global_type_info const* deserializer::begin_object()
{
  VAST_ENTER();
  type_id id = 0;
  load(*this, id);
  auto ti = detail::type_manager::instance()->lookup(id);
  VAST_RETURN(ti);
}

bool deserializer::end_object()
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

binary_serializer::binary_serializer(io::output_stream& sink)
  : sink_(sink)
{
}

bool binary_serializer::typed() const
{
  return true;
}

bool binary_serializer::begin_object(global_type_info const&)
{
  VAST_ENTER();
  VAST_RETURN(true);
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

bool binary_deserializer::typed() const
{
  return true;
}

global_type_info const* binary_deserializer::begin_object()
{
  VAST_ENTER();
  VAST_RETURN(nullptr);
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

} // namespace vast
