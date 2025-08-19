//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ip.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/concept/printable/tenzir/ip.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/word.hpp"

#include <arpa/inet.h>
#include <fmt/format.h>
#include <netinet/in.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <sys/socket.h>

#include <cstdlib>
#include <string_view>
#include <utility>

namespace tenzir {

namespace {

inline auto bitmask32(size_t bottom_bits) -> uint32_t {
  return bottom_bits >= 32 ? 0xffffffff : ((uint32_t{1} << bottom_bits) - 1);
}

class address_encryptor {
public:
  explicit address_encryptor(const std::array<ip::byte_type, 32>& key) {
    cipher_ = EVP_get_cipherbyname("aes-128-ecb");
    block_size_ = EVP_CIPHER_block_size(cipher_);
    pad_ = std::vector<ip::byte_type>(block_size_);
    auto pad_out_len = 0;
    EVP_CipherInit_ex(ctx_.get(), cipher_, nullptr, key.data(), nullptr, 1);
    // use second 16-byte half of key for padding
    EVP_CipherUpdate(ctx_.get(), pad_.data(), &pad_out_len,
                     key.data() + block_size_, block_size_);
  }

  auto operator()(ip::byte_array bytes, size_t byte_offset) {
    auto bytes_to_encrypt = std::span{bytes.begin() + byte_offset, bytes.end()};
    auto one_time_pad = generate_one_time_pad(bytes_to_encrypt);
    for (auto i = size_t{0}; i < bytes_to_encrypt.size(); ++i) {
      bytes[i + byte_offset] = bytes_to_encrypt[i] ^ one_time_pad[i];
    }
    return bytes;
  }

private:
  static constexpr inline auto msb_of_byte_mask = 0b10000000;
  std::unique_ptr<EVP_CIPHER_CTX, std::function<void(EVP_CIPHER_CTX*)>> ctx_
    = {EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free};
  const EVP_CIPHER* cipher_ = {};
  int block_size_ = {};
  std::vector<ip::byte_type> pad_ = {};

  auto generate_one_time_pad(std::span<ip::byte_type> bytes_to_encrypt)
    -> std::vector<ip::byte_type> {
    auto out_len = 0;
    auto cipher_input = std::vector<ip::byte_type>(pad_);
    auto cipher_output = std::vector<ip::byte_type>(pad_);
    EVP_CipherUpdate(ctx_.get(), cipher_output.data(), &out_len,
                     cipher_input.data(), block_size_);
    auto byte_index = 0;
    auto bit_index = 0;
    auto one_time_pad = std::vector<ip::byte_type>(bytes_to_encrypt.size());
    one_time_pad[byte_index] |= cipher_output[0] & msb_of_byte_mask;
    for (auto i = size_t{0}; i < bytes_to_encrypt.size() * 8 - 1;) {
      auto padding_mask = 0xff >> (bit_index + 1);
      auto original_mask = ~padding_mask;
      auto padding_byte = pad_[byte_index];
      auto original_byte = bytes_to_encrypt[byte_index];
      cipher_input[byte_index]
        = (original_byte & original_mask) | (padding_byte & padding_mask);
      EVP_CipherUpdate(ctx_.get(), cipher_output.data(), &out_len,
                       cipher_input.data(), block_size_);
      ++i;
      byte_index = i / 8;
      bit_index = i % 8;
      one_time_pad[byte_index]
        |= (cipher_output[0] & msb_of_byte_mask) >> bit_index;
    }
    return one_time_pad;
  }
};

} // namespace

auto ip::pseudonymize(
  const ip& original,
  const std::array<byte_type, pseudonymization_seed_array_size>& seed) -> ip {
  auto byte_offset = (original.is_v4() ? 12 : 0);
  address_encryptor encryptor(seed);
  auto pseudonymized_bytes = encryptor(original.bytes_, byte_offset);
  return ip(pseudonymized_bytes);
}

auto ip::is_v4() const -> bool {
  return std::memcmp(&bytes_, &v4_mapped_prefix, 12) == 0;
}

auto ip::is_v6() const -> bool {
  return !is_v4();
}

auto ip::is_loopback() const -> bool {
  if (is_v4()) {
    return bytes_[12] == 127;
  } else {
    return ((bytes_[0] == 0) && (bytes_[1] == 0) && (bytes_[2] == 0)
            && (bytes_[3] == 0) && (bytes_[4] == 0) && (bytes_[5] == 0)
            && (bytes_[6] == 0) && (bytes_[7] == 0) && (bytes_[8] == 0)
            && (bytes_[9] == 0) && (bytes_[10] == 0) && (bytes_[11] == 0)
            && (bytes_[12] == 0) && (bytes_[13] == 0) && (bytes_[14] == 0)
            && (bytes_[15] == 1));
  }
}

auto ip::is_broadcast() const -> bool {
  return is_v4() && bytes_[12] == 0xff && bytes_[13] == 0xff
         && bytes_[14] == 0xff && bytes_[15] == 0xff;
}

auto ip::is_multicast() const -> bool {
  return is_v4() ? (bytes_[12] >= 224 && bytes_[12] <= 239) : bytes_[0] == 0xff;
}

auto ip::is_private() const -> bool {
  if (is_v4()) {
    // IPv4 private addresses:
    // 10.0.0.0/8 (10.0.0.0 - 10.255.255.255)
    // 172.16.0.0/12 (172.16.0.0 - 172.31.255.255)
    // 192.168.0.0/16 (192.168.0.0 - 192.168.255.255)
    // Note: 169.254.0.0/16 (link-local) is NOT included here
    return (bytes_[12] == 10)
           || (bytes_[12] == 172 && bytes_[13] >= 16 && bytes_[13] <= 31)
           || (bytes_[12] == 192 && bytes_[13] == 168);
  } else {
    // IPv6 private addresses:
    // fc00::/7 - Unique Local Addresses (ULA)
    // Note: fe80::/10 (link-local) is NOT included here
    return (bytes_[0] >= 0xfc && bytes_[0] <= 0xfd);
  }
}

auto ip::is_global() const -> bool {
  // A global address is one that is not:
  // - loopback
  // - private
  // - link-local
  // - multicast
  // - broadcast (IPv4 only)
  // - unspecified (0.0.0.0 or ::)

  // Check for unspecified address
  if (is_v4()) {
    // For IPv4, check if the last 4 bytes are all zero (0.0.0.0)
    if (bytes_[12] == 0 && bytes_[13] == 0 && bytes_[14] == 0
        && bytes_[15] == 0) {
      return false;
    }
  } else {
    // For IPv6, check if all bytes are zero (::)
    bool is_unspecified = true;
    for (int i = 0; i < 16; ++i) {
      if (bytes_[i] != 0) {
        is_unspecified = false;
        break;
      }
    }
    if (is_unspecified) {
      return false;
    }
  }

  return ! is_loopback() && ! is_private() && ! is_link_local()
         && ! is_multicast() && ! is_broadcast();
}

auto ip::is_link_local() const -> bool {
  if (is_v4()) {
    // IPv4 link-local: 169.254.0.0/16
    return bytes_[12] == 169 && bytes_[13] == 254;
  } else {
    // IPv6 link-local: fe80::/10
    return (bytes_[0] == 0xfe) && ((bytes_[1] & 0xc0) == 0x80);
  }
}

auto ip::type() const -> ip_address_class {
  // Check for unspecified address first (0.0.0.0 or ::)
  if (is_v4()) {
    // For IPv4, check if the last 4 bytes are all zero (0.0.0.0)
    if (bytes_[12] == 0 && bytes_[13] == 0 && bytes_[14] == 0
        && bytes_[15] == 0) {
      return ip_address_class::unspecified;
    }
  } else {
    // For IPv6, check if all bytes are zero (::)
    bool is_unspecified = true;
    for (int i = 0; i < 16; ++i) {
      if (bytes_[i] != 0) {
        is_unspecified = false;
        break;
      }
    }
    if (is_unspecified) {
      return ip_address_class::unspecified;
    }
  }

  // Check in order of specificity
  if (is_loopback()) {
    return ip_address_class::loopback;
  }
  if (is_link_local()) {
    return ip_address_class::link_local;
  }
  if (is_multicast()) {
    return ip_address_class::multicast;
  }
  if (is_broadcast()) {
    return ip_address_class::broadcast;
  }
  if (is_private()) {
    return ip_address_class::private_;
  }
  // Everything else is global
  return ip_address_class::global;
}

auto ip::mask(unsigned top_bits_to_keep) -> bool {
  if (top_bits_to_keep > 128) {
    return false;
  }
  uint32_t m[4] = {0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff};
  auto r = std::ldiv(top_bits_to_keep, 32);
  if (r.quot < 4) {
    m[r.quot] = detail::to_network_order(m[r.quot] & ~bitmask32(32 - r.rem));
  }
  for (size_t i = r.quot + 1; i < 4; ++i) {
    m[i] = 0;
  }
  auto p = reinterpret_cast<uint32_t*>(std::launder(&bytes_[0]));
  for (size_t i = 0; i < 4; ++i) {
    p[i] &= m[i];
  }
  return true;
}

auto ip::operator&=(const ip& other) -> ip& {
  for (auto i = 0u; i < 16u; ++i) {
    bytes_[i] &= other.bytes_[i];
  }
  return *this;
}

auto ip::operator|=(const ip& other) -> ip& {
  for (auto i = 0u; i < 16u; ++i) {
    bytes_[i] |= other.bytes_[i];
  }
  return *this;
}

auto ip::operator^=(const ip& other) -> ip& {
  if (is_v4() || other.is_v4()) {
    for (auto i = 12u; i < 16u; ++i) {
      bytes_[i] ^= other.bytes_[i];
    }
  } else {
    for (auto i = 0u; i < 16u; ++i) {
      bytes_[i] ^= other.bytes_[i];
    }
  }
  return *this;
}

auto ip::compare(const ip& other, size_t k) const -> bool {
  TENZIR_ASSERT(k <= 128);
  if (k == 0) { // trivially true
    return true;
  }
  auto x = bytes_.data();
  auto y = other.bytes_.data();
  for (; k > 8; k -= 8) {
    if (*x++ != *y++) {
      return false;
    }
  }
  auto mask = word<byte_type>::msb_fill(k);
  return (*x & mask) == (*y & mask);
}

} // namespace tenzir

auto fmt::formatter<tenzir::ip>::format(const tenzir::ip& value,
                                        format_context& ctx) const
  -> format_context::iterator {
  auto buffer = std::array<char, INET6_ADDRSTRLEN>{};
  buffer.fill(0);
  auto bytes = as_bytes(value);
  if (value.is_v4()) {
    const auto* result
      = inet_ntop(AF_INET, &bytes[12], buffer.data(), INET_ADDRSTRLEN);
    TENZIR_ASSERT(result != nullptr);
  } else {
    const auto* result
      = inet_ntop(AF_INET6, bytes.data(), buffer.data(), INET6_ADDRSTRLEN);
    TENZIR_ASSERT(result != nullptr);
  }
  auto str = std::string_view{buffer.data()};
  return formatter<string_view>::format(str, ctx);
}
