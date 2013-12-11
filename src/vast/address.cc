#include "vast/address.h"

#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include "vast/logger.h"
#include "vast/string.h"
#include "vast/serialization.h"
#include "vast/util/byte_swap.h"

std::array<uint8_t, 12> const vast::address::v4_mapped_prefix =
    {{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff }};

namespace vast {

optional<address> address::from_string(char const* str)
{
  auto p = str;
  while (*p != '\0' && *p != ':')
    ++p;
  return *p != '\0' ? from_v6(str) : from_v4(str);
}

optional<address> address::from_string(std::string const& str)
{
  if (str.size() > 63)
    return {};
  return from_string(str.c_str());
}

optional<address> address::from_string(string const& str)
{
  return from_string(str.data());
}

optional<address> address::from_v4(char const* str)
{
  address r;
  std::copy(v4_mapped_prefix.begin(), v4_mapped_prefix.end(), r.bytes_.begin());

  int a[4];
  int n = sscanf(str, "%d.%d.%d.%d", a + 0, a + 1, a + 2, a + 3);

  if (n != 4
      || a[0] < 0 || a[1] < 0 || a[2] < 0 || a[3] < 0
      || a[0] > 255 || a[1] > 255 || a[2] > 255 || a[3] > 255)
    return {};

  uint32_t addr = (a[0] << 24) | (a[1] << 16) | (a[2] << 8) | a[3];
  auto p = reinterpret_cast<uint32_t*>(&r.bytes_[12]);
  *p = util::byte_swap<host_endian, network_endian>(addr);

  return {std::move(r)};
}

optional<address> address::from_v6(char const* str)
{
  address r;
  if (inet_pton(AF_INET6, str, r.bytes_.data()) <= 0)
    return {};

  return {std::move(r)};
}

address::address()
{
  bytes_.fill(0);
}

address::address(char const* str)
{
  if (auto a = from_string(str))
    *this = std::move(*a);
}

address::address(std::string const& str)
  : address{str.c_str()}
{
}

address::address(string const& str)
  : address{str.data()}
{
}

address::address(address&& other)
  : bytes_(std::move(other.bytes_))
{
  other.bytes_.fill(0);
}

address::address(uint32_t const* bytes, family fam, byte_order order)
{
  if (fam == ipv4)
  {
    std::copy(v4_mapped_prefix.begin(), v4_mapped_prefix.end(), bytes_.begin());

    auto p = reinterpret_cast<uint32_t*>(&bytes_[12]);
    if (order == host)
      *p = util::byte_swap<host_endian, network_endian>(*bytes);
    else
      *p = *bytes;
  }
  else if (order == host)
  {
    for (auto i = 0u; i < 4u; ++i)
    {
      auto p = reinterpret_cast<uint32_t*>(&bytes_[i * 4]);
      *p = util::byte_swap<host_endian, network_endian>(*(bytes + i));
    }
  }
  else
  {
    std::copy(bytes, bytes + 4, reinterpret_cast<uint32_t*>(bytes_.data()));
  }
}

bool address::is_v4() const
{
  return std::memcmp(&bytes_, &v4_mapped_prefix, 12) == 0;
}

bool address::is_v6() const
{
  return ! is_v4();
}

bool address::is_loopback() const
{
  if (is_v4())
    return bytes_[12] == 127;
  else
    return ((bytes_[0] == 0) && (bytes_[1] == 0) &&
            (bytes_[2] == 0) && (bytes_[3] == 0) &&
            (bytes_[4] == 0) && (bytes_[5] == 0) &&
            (bytes_[6] == 0) && (bytes_[7] == 0) &&
            (bytes_[8] == 0) && (bytes_[9] == 0) &&
            (bytes_[10] == 0) && (bytes_[11] == 0) &&
            (bytes_[12] == 0) && (bytes_[13] == 0) &&
            (bytes_[14] == 0) && (bytes_[15] == 1));
}

bool address::is_broadcast() const
{
  return is_v4() &&
    bytes_[12] == 0xff && bytes_[13] == 0xff &&
    bytes_[14] == 0xff && bytes_[15] == 0xff;
}

bool address::is_multicast() const
{
  return is_v4() ? bytes_[12] == 224 : bytes_[0] == 0xff;
}

bool address::mask(unsigned top_bits_to_keep)
{
  if (top_bits_to_keep > 128)
    return false;

  auto i = 12u;
  auto bits_to_chop = 128 - top_bits_to_keep;
  while (bits_to_chop >= 32)
  {
    auto p = reinterpret_cast<uint32_t*>(&bytes_[i]);
    *p = 0;
    bits_to_chop -= 32;
    i -= 4;
  }

  auto last = reinterpret_cast<uint32_t*>(&bytes_[i]);
  auto val = util::byte_swap<network_endian, host_endian>(*last);
  val >>= bits_to_chop;
  val <<= bits_to_chop;
  *last = util::byte_swap<host_endian, network_endian>(val);

  return true;
}

address& address::operator&=(address const& other)
{
  for (auto i = 0u; i < 16u; ++i)
    bytes_[i] &= other.bytes_[i];
  return *this;
}

address& address::operator|=(address const& other)
{
  for (auto i = 0u; i < 16u; ++i)
    bytes_[i] |= other.bytes_[i];
  return *this;
}

address& address::operator^=(address const& other)
{
  if (is_v4() || other.is_v4())
    for (auto i = 12u; i < 16u; ++i)
      bytes_[i] ^= other.bytes_[i];
  else
    for (auto i = 0u; i < 16u; ++i)
      bytes_[i] ^= other.bytes_[i];

  return *this;
}

std::array<uint8_t, 16> const& address::data() const
{
  return bytes_;
}

void address::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  for (size_t i = 0; i < 16; i += 8)
  {
    auto p = reinterpret_cast<uint64_t const*>(&bytes_[i]);
    sink << *p;
  }
}

void address::deserialize(deserializer& source)
{
  VAST_ENTER();
  for (size_t i = 0; i < 16; i += 8)
  {
    auto p = reinterpret_cast<uint64_t*>(&bytes_[i]);
    source >> *p;
  }
  VAST_LEAVE(VAST_THIS);
}

bool address::convert(string& str) const
{
  if (is_v4())
  {
    char buf[INET_ADDRSTRLEN];
    if (inet_ntop(AF_INET, &bytes_[12], buf, INET_ADDRSTRLEN) == nullptr)
      return false;
    str = {buf};
  }
  else
  {
    char buf[INET6_ADDRSTRLEN];
    if (inet_ntop(AF_INET6, &bytes_, buf, INET6_ADDRSTRLEN) == nullptr)
      return false;
    str = {buf};
  }

  return true;
}

bool operator==(address const& x, address const& y)
{
  return x.bytes_ == y.bytes_;
}

bool operator<(address const& x, address const& y)
{
  return x.bytes_ < y.bytes_;
}

} // namespace vast
