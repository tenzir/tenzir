#ifndef VAST_DETAIL_CPPA_SERIALIZATION_H
#define VAST_DETAIL_CPPA_SERIALIZATION_H

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>
#include "vast/serialization.h"
#include "vast/util/byte_swap.h"

namespace vast {
namespace detail {

class caf_serializer : public serializer
{
public:
  caf_serializer(caf::serializer* sink);
  virtual ~caf_serializer();
  virtual bool begin_sequence(uint64_t size) override;
  virtual bool end_sequence() override;
  virtual bool write_bool(bool x) override;
  virtual bool write_int8(int8_t x) override;
  virtual bool write_uint8(uint8_t x) override;
  virtual bool write_int16(int16_t x) override;
  virtual bool write_uint16(uint16_t x) override;
  virtual bool write_int32(int32_t x) override;
  virtual bool write_uint32(uint32_t x) override;
  virtual bool write_int64(int64_t x) override;
  virtual bool write_uint64(uint64_t x) override;
  virtual bool write_double(double x) override;
  virtual bool write_raw(void const* data, size_t size) override;
  size_t bytes() const;

private:
  template <typename T>
  std::enable_if_t<std::is_arithmetic<T>::value, bool>
  write(T x)
  {
    sink_->write_value(util::byte_swap<host_endian, network_endian>(x));
    bytes_ += sizeof(T);
    return true;
  }

  caf::serializer* sink_;
  size_t bytes_;
};

class caf_deserializer : public deserializer
{
public:
  caf_deserializer(caf::deserializer* source);
  virtual ~caf_deserializer();
  virtual bool begin_sequence(uint64_t& size) override;
  virtual bool end_sequence() override;
  virtual bool read_bool(bool& x) override;
  virtual bool read_int8(int8_t& x) override;
  virtual bool read_uint8(uint8_t& x) override;
  virtual bool read_int16(int16_t& x) override;
  virtual bool read_uint16(uint16_t& x) override;
  virtual bool read_int32(int32_t& x) override;
  virtual bool read_uint32(uint32_t& x) override;
  virtual bool read_int64(int64_t& x) override;
  virtual bool read_uint64(uint64_t& x) override;
  virtual bool read_double(double& x) override;
  virtual bool read_raw(void* data, size_t size) override;
  size_t bytes() const;

private:
  template <typename T>
  std::enable_if_t<std::is_arithmetic<T>::value, bool>
  read(T& x)
  {
    x = source_->read<T>();
    x = util::byte_swap<network_endian, host_endian>(x);
    bytes_ += sizeof(T);
    return true;
  }

  caf::deserializer* source_;
  size_t bytes_;
};

} // namespace detail
} // namespace vast

#endif
