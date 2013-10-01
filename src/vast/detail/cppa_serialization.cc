#include <vast/detail/cppa_serialization.h>

namespace vast {
namespace detail {

cppa_serializer::cppa_serializer(cppa::serializer* sink)
  : sink_(sink)
{
}

cppa_serializer::~cppa_serializer()
{
}

bool cppa_serializer::begin_sequence(uint64_t size)
{
  return write(size);
}

bool cppa_serializer::end_sequence()
{
  // Do nothing.
  return true;
}

bool cppa_serializer::write_bool(bool x)
{
  return write(static_cast<uint8_t>(x));
}
  
bool cppa_serializer::write_int8(int8_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_uint8(uint8_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_int16(int16_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_uint16(uint16_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_int32(int32_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_uint32(uint32_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_int64(int64_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_uint64(uint64_t x)
{
  return write(x);
}
  
bool cppa_serializer::write_double(double x)
{
  return write(x);
}
  
bool cppa_serializer::write_raw(void const* data, size_t size)
{
  sink_->write_raw(size, data);
  bytes_ += size;
  return true;
}

size_t cppa_serializer::bytes() const
{
  return bytes_;
}


cppa_deserializer::cppa_deserializer(cppa::deserializer* source)
  : source_(source)
{
}

cppa_deserializer::~cppa_deserializer()
{
}

bool cppa_deserializer::begin_sequence(uint64_t& size)
{
  return read(size);
}

bool cppa_deserializer::end_sequence()
{
  // Do nothing.
  return true;
}

bool cppa_deserializer::read_bool(bool& x)
{
  uint8_t u;
  auto success = read(u);
  x = u == 1;
  bytes_ += sizeof(u);
  return success;
}
  
bool cppa_deserializer::read_int8(int8_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_uint8(uint8_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_int16(int16_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_uint16(uint16_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_int32(int32_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_uint32(uint32_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_int64(int64_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_uint64(uint64_t& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_double(double& x)
{
  return read(x);
}
  
bool cppa_deserializer::read_raw(void* data, size_t size)
{
  source_->read_raw(size, data);
  bytes_ += size;
  return true;
}

size_t cppa_deserializer::bytes() const
{
  return bytes_;
}

} // namespace detail
} // namespace vast
