#include <vast/detail/caf_serialization.h>

namespace vast {
namespace detail {

caf_serializer::caf_serializer(caf::serializer* sink)
  : sink_(sink)
{
}

caf_serializer::~caf_serializer()
{
}

bool caf_serializer::begin_sequence(uint64_t size)
{
  return write(size);
}

bool caf_serializer::end_sequence()
{
  // Do nothing.
  return true;
}

bool caf_serializer::write_bool(bool x)
{
  return write(static_cast<uint8_t>(x));
}

bool caf_serializer::write_int8(int8_t x)
{
  return write(x);
}

bool caf_serializer::write_uint8(uint8_t x)
{
  return write(x);
}

bool caf_serializer::write_int16(int16_t x)
{
  return write(x);
}

bool caf_serializer::write_uint16(uint16_t x)
{
  return write(x);
}

bool caf_serializer::write_int32(int32_t x)
{
  return write(x);
}

bool caf_serializer::write_uint32(uint32_t x)
{
  return write(x);
}

bool caf_serializer::write_int64(int64_t x)
{
  return write(x);
}

bool caf_serializer::write_uint64(uint64_t x)
{
  return write(x);
}

bool caf_serializer::write_double(double x)
{
  return write(x);
}

bool caf_serializer::write_raw(void const* data, size_t size)
{
  sink_->write_raw(size, data);
  bytes_ += size;
  return true;
}

size_t caf_serializer::bytes() const
{
  return bytes_;
}


caf_deserializer::caf_deserializer(caf::deserializer* source)
  : source_(source)
{
}

caf_deserializer::~caf_deserializer()
{
}

bool caf_deserializer::begin_sequence(uint64_t& size)
{
  return read(size);
}

bool caf_deserializer::end_sequence()
{
  // Do nothing.
  return true;
}

bool caf_deserializer::read_bool(bool& x)
{
  uint8_t u;
  auto success = read(u);
  x = u == 1;
  bytes_ += sizeof(u);
  return success;
}

bool caf_deserializer::read_int8(int8_t& x)
{
  return read(x);
}

bool caf_deserializer::read_uint8(uint8_t& x)
{
  return read(x);
}

bool caf_deserializer::read_int16(int16_t& x)
{
  return read(x);
}

bool caf_deserializer::read_uint16(uint16_t& x)
{
  return read(x);
}

bool caf_deserializer::read_int32(int32_t& x)
{
  return read(x);
}

bool caf_deserializer::read_uint32(uint32_t& x)
{
  return read(x);
}

bool caf_deserializer::read_int64(int64_t& x)
{
  return read(x);
}

bool caf_deserializer::read_uint64(uint64_t& x)
{
  return read(x);
}

bool caf_deserializer::read_double(double& x)
{
  return read(x);
}

bool caf_deserializer::read_raw(void* data, size_t size)
{
  source_->read_raw(size, data);
  bytes_ += size;
  return true;
}

size_t caf_deserializer::bytes() const
{
  return bytes_;
}

} // namespace detail
} // namespace vast
