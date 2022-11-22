//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/address.hpp"

#include "vast/as_bytes.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/address.hpp"
#include "vast/data.hpp"
#include "vast/word.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <sys/socket.h>

#include <cstdlib>
#include <utility>

namespace vast {

namespace {

inline uint32_t bitmask32(size_t bottom_bits) {
  return bottom_bits >= 32 ? 0xffffffff : ((uint32_t{1} << bottom_bits) - 1);
}

class address_encryptor {
public:
  explicit address_encryptor(const std::array<address::byte_type, 32>& key) {
    cipher_ = EVP_get_cipherbyname("aes-128-ecb");
    block_size_ = EVP_CIPHER_block_size(cipher_);
    pad_ = std::vector<address::byte_type>(block_size_);
    auto pad_out_len = 0;
    EVP_CipherInit_ex(ctx_.get(), cipher_, nullptr, key.data(), nullptr, 1);
    // use second 16-byte half of key for padding
    EVP_CipherUpdate(ctx_.get(), pad_.data(), &pad_out_len,
                     key.data() + block_size_, block_size_);
  }

  auto operator()(address::byte_array bytes, size_t byte_offset) {
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
  std::vector<address::byte_type> pad_ = {};

  std::vector<address::byte_type>
  generate_one_time_pad(std::span<address::byte_type> bytes_to_encrypt) {
    auto out_len = 0;
    auto cipher_input = std::vector<address::byte_type>(pad_);
    auto cipher_output = std::vector<address::byte_type>(pad_);
    EVP_CipherUpdate(ctx_.get(), cipher_output.data(), &out_len,
                     cipher_input.data(), block_size_);
    auto byte_index = 0;
    auto bit_index = 0;
    auto one_time_pad
      = std::vector<address::byte_type>(bytes_to_encrypt.size());
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

address address::pseudonymize(
  const address& original,
  const std::array<byte_type, pseudonymization_seed_array_size>& seed) {
  auto byte_offset = (original.is_v4() ? 12 : 0);
  address_encryptor encryptor(seed);
  auto pseudonymized_bytes = encryptor(original.bytes_, byte_offset);
  return address(pseudonymized_bytes);
}

bool address::is_v4() const {
  return std::memcmp(&bytes_, &v4_mapped_prefix, 12) == 0;
}

bool address::is_v6() const {
  return !is_v4();
}

bool address::is_loopback() const {
  if (is_v4())
    return bytes_[12] == 127;
  else
    return ((bytes_[0] == 0) && (bytes_[1] == 0) && (bytes_[2] == 0)
            && (bytes_[3] == 0) && (bytes_[4] == 0) && (bytes_[5] == 0)
            && (bytes_[6] == 0) && (bytes_[7] == 0) && (bytes_[8] == 0)
            && (bytes_[9] == 0) && (bytes_[10] == 0) && (bytes_[11] == 0)
            && (bytes_[12] == 0) && (bytes_[13] == 0) && (bytes_[14] == 0)
            && (bytes_[15] == 1));
}

bool address::is_broadcast() const {
  return is_v4() && bytes_[12] == 0xff && bytes_[13] == 0xff
         && bytes_[14] == 0xff && bytes_[15] == 0xff;
}

bool address::is_multicast() const {
  return is_v4() ? bytes_[12] == 224 : bytes_[0] == 0xff;
}

bool address::mask(unsigned top_bits_to_keep) {
  if (top_bits_to_keep > 128)
    return false;
  uint32_t m[4] = {0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff};
  auto r = std::ldiv(top_bits_to_keep, 32);
  if (r.quot < 4)
    m[r.quot] = detail::to_network_order(m[r.quot] & ~bitmask32(32 - r.rem));
  for (size_t i = r.quot + 1; i < 4; ++i)
    m[i] = 0;
  auto p = reinterpret_cast<uint32_t*>(std::launder(&bytes_[0]));
  for (size_t i = 0; i < 4; ++i)
    p[i] &= m[i];
  return true;
}

address& address::operator&=(const address& other) {
  for (auto i = 0u; i < 16u; ++i)
    bytes_[i] &= other.bytes_[i];
  return *this;
}

address& address::operator|=(const address& other) {
  for (auto i = 0u; i < 16u; ++i)
    bytes_[i] |= other.bytes_[i];
  return *this;
}

address& address::operator^=(const address& other) {
  if (is_v4() || other.is_v4())
    for (auto i = 12u; i < 16u; ++i)
      bytes_[i] ^= other.bytes_[i];
  else
    for (auto i = 0u; i < 16u; ++i)
      bytes_[i] ^= other.bytes_[i];
  return *this;
}

bool address::compare(const address& other, size_t k) const {
  VAST_ASSERT(k > 0 && k <= 128);
  auto x = bytes_.data();
  auto y = other.bytes_.data();
  for (; k > 8; k -= 8)
    if (*x++ != *y++)
      return false;
  auto mask = word<byte_type>::msb_fill(k);
  return (*x & mask) == (*y & mask);
}

bool operator==(const address& x, const address& y) {
  return x.bytes_ == y.bytes_;
}

bool operator<(const address& x, const address& y) {
  return x.bytes_ < y.bytes_;
}

} // namespace vast
