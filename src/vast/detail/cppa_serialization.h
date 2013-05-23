#ifndef VAST_DETAIL_CPPA_SERIALIZATION_H
#define VAST_DETAIL_CPPA_SERIALIZATION_H

#include <cppa/deserializer.hpp>
#include <cppa/serializer.hpp>
#include "vast/io/serialization.h"
#include "vast/util/byte_swap.h"

namespace vast {
namespace detail {

class cppa_serializer : public io::serializer
{
public:
  cppa_serializer(cppa::serializer* sink, std::string const& name);
  ~cppa_serializer();
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
  virtual bool write_sequence_begin(uint64_t size) override;
  virtual bool write_sequence_end() override;
  virtual bool write_raw(void const* data, size_t size) override;
  virtual size_t bytes() const;

private:
  template <typename T>
  typename std::enable_if<std::is_arithmetic<T>::value, bool>::type
  write(T x)
  {
    using namespace vast;
    sink_->write_value(util::byte_swap<host_endian, network_endian>(x));
    bytes_ += sizeof(T);
    return true;
  }

  cppa::serializer* sink_;
  size_t bytes_;
};

class cppa_deserializer : public io::deserializer
{
public:
  cppa_deserializer(cppa::deserializer* source, std::string const& name);
  ~cppa_deserializer();
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
  virtual bool read_sequence_begin(uint64_t& size) override;
  virtual bool read_sequence_end() override;
  virtual bool read_raw(void* data, size_t size) override;
  virtual size_t bytes() const;

private:
  template <typename T>
  typename std::enable_if<std::is_arithmetic<T>::value, bool>::type
  read(T& x)
  {
    using namespace vast;
    using namespace cppa::detail;
    static constexpr auto ptype = type_to_ptype<T>::ptype;
    static_assert(
        std::is_same<T, typename ptype_to_type<ptype>::type>::value,
        "invalid type conversion on cppa's primitive variant");

    auto pv = source_->read_value(ptype);
    x = util::byte_swap<network_endian, host_endian>(cppa::get<T>(pv));
    bytes_ += sizeof(T);
    return true;
  }

  cppa::deserializer* source_;
  size_t bytes_;
};

} // namespace detail
} // namespace vast

#endif
