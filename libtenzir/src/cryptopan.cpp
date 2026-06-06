//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/cryptopan.hpp"

#include <openssl/evp.h>
#include <openssl/ossl_typ.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace tenzir {

namespace {

using byte_array = std::array<uint8_t, 16>;
using seed_bytes = std::array<uint8_t, cryptopan_seed_size>;

auto openssl_bytes(uint8_t* bytes) -> unsigned char* {
  return reinterpret_cast<unsigned char*>(bytes);
}

auto openssl_bytes(uint8_t const* bytes) -> unsigned char const* {
  return reinterpret_cast<unsigned char const*>(bytes);
}

auto to_seed_bytes(cryptopan_seed const& seed) -> seed_bytes {
  auto result = seed_bytes{};
  for (auto i = size_t{0}; i < result.size(); ++i) {
    result[i] = std::to_integer<uint8_t>(seed[i]);
  }
  return result;
}

auto to_byte_array(ip const& address) -> byte_array {
  auto result = byte_array{};
  std::ranges::copy(as_bytes<uint8_t>(address), result.begin());
  return result;
}

class cryptopan_cipher {
public:
  explicit cryptopan_cipher(seed_bytes const& key) {
    cipher_ = EVP_get_cipherbyname("aes-128-ecb");
    block_size_ = EVP_CIPHER_block_size(cipher_);
    pad_ = std::vector<uint8_t>(block_size_);
    auto pad_out_len = 0;
    EVP_CipherInit_ex(ctx_.get(), cipher_, nullptr, openssl_bytes(key.data()),
                      nullptr, 1);
    // Use the second 16-byte half of the key for padding.
    EVP_CipherUpdate(ctx_.get(), openssl_bytes(pad_.data()), &pad_out_len,
                     openssl_bytes(key.data() + block_size_), block_size_);
  }

  auto encrypt(byte_array bytes, size_t byte_offset) -> byte_array {
    auto bytes_to_encrypt = std::span{bytes}.subspan(byte_offset);
    auto one_time_pad = generate_one_time_pad(bytes_to_encrypt);
    for (auto i = size_t{0}; i < bytes_to_encrypt.size(); ++i) {
      bytes_to_encrypt[i] ^= one_time_pad[i];
    }
    return bytes;
  }

  auto decrypt(byte_array bytes, size_t byte_offset) -> byte_array {
    auto bytes_to_decrypt = std::span{bytes}.subspan(byte_offset);
    auto ciphertext
      = std::vector<uint8_t>{bytes_to_decrypt.begin(), bytes_to_decrypt.end()};
    auto plaintext = std::vector<uint8_t>(bytes_to_decrypt.size());
    auto cipher_input = std::vector<uint8_t>(pad_);
    auto pad_bit = generate_pad_bit(cipher_input);
    for (auto i = size_t{0}; i < bytes_to_decrypt.size() * 8; ++i) {
      auto plaintext_bit = get_bit(ciphertext, i) != pad_bit;
      set_bit(plaintext, i, plaintext_bit);
      set_bit(bytes_to_decrypt, i, plaintext_bit);
      if (i + 1 == bytes_to_decrypt.size() * 8) {
        break;
      }
      update_cipher_input(cipher_input, plaintext, i);
      pad_bit = generate_pad_bit(cipher_input);
    }
    return bytes;
  }

private:
  static constexpr auto msb_of_byte_mask = uint8_t{0b10000000};
  std::unique_ptr<EVP_CIPHER_CTX, std::function<void(EVP_CIPHER_CTX*)>> ctx_
    = {EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free};
  EVP_CIPHER const* cipher_ = {};
  int block_size_ = {};
  std::vector<uint8_t> pad_ = {};

  static auto get_bit(std::span<uint8_t const> bytes, size_t bit) -> bool {
    auto byte_index = bit / 8;
    auto bit_index = bit % 8;
    auto mask = msb_of_byte_mask >> bit_index;
    return (bytes[byte_index] & mask) != 0;
  }

  static auto set_bit(std::span<uint8_t> bytes, size_t bit, bool value)
    -> void {
    auto byte_index = bit / 8;
    auto bit_index = bit % 8;
    auto mask = msb_of_byte_mask >> bit_index;
    if (not value) {
      bytes[byte_index] &= ~mask;
    } else {
      bytes[byte_index] |= mask;
    }
  }

  auto generate_pad_bit(std::span<uint8_t const> cipher_input) -> bool {
    auto out_len = 0;
    auto cipher_output = std::vector<uint8_t>(pad_);
    EVP_CipherUpdate(ctx_.get(), openssl_bytes(cipher_output.data()), &out_len,
                     openssl_bytes(cipher_input.data()), block_size_);
    return (cipher_output[0] & msb_of_byte_mask) != 0;
  }

  auto update_cipher_input(std::span<uint8_t> cipher_input,
                           std::span<uint8_t const> plaintext, size_t bit) const
    -> void {
    auto byte_index = bit / 8;
    auto bit_index = bit % 8;
    auto padding_mask = static_cast<uint8_t>(0xff >> (bit_index + 1));
    auto original_mask = ~padding_mask;
    auto padding_byte = pad_[byte_index];
    auto original_byte = plaintext[byte_index];
    cipher_input[byte_index]
      = (original_byte & original_mask) | (padding_byte & padding_mask);
  }

  auto generate_one_time_pad(std::span<uint8_t const> bytes_to_encrypt)
    -> std::vector<uint8_t> {
    auto cipher_input = std::vector<uint8_t>(pad_);
    auto byte_index = size_t{0};
    auto bit_index = size_t{0};
    auto one_time_pad = std::vector<uint8_t>(bytes_to_encrypt.size());
    if (bytes_to_encrypt.empty()) {
      return one_time_pad;
    }
    set_bit(one_time_pad, 0, generate_pad_bit(cipher_input));
    for (auto i = size_t{0}; i < bytes_to_encrypt.size() * 8 - 1;) {
      auto padding_mask = static_cast<uint8_t>(0xff >> (bit_index + 1));
      auto original_mask = ~padding_mask;
      auto padding_byte = pad_[byte_index];
      auto original_byte = bytes_to_encrypt[byte_index];
      cipher_input[byte_index]
        = (original_byte & original_mask) | (padding_byte & padding_mask);
      ++i;
      byte_index = i / 8;
      bit_index = i % 8;
      set_bit(one_time_pad, i, generate_pad_bit(cipher_input));
    }
    return one_time_pad;
  }
};

auto byte_offset(ip::family family) -> size_t {
  return family == ip::ipv4 ? 12 : 0;
}

auto inferred_family(ip const& address) -> ip::family {
  return address.is_v4() ? ip::ipv4 : ip::ipv6;
}

} // namespace

auto encrypt_cryptopan(ip const& address, cryptopan_seed const& seed) -> ip {
  auto cipher = cryptopan_cipher{to_seed_bytes(seed)};
  auto encrypted_bytes = cipher.encrypt(to_byte_array(address),
                                        byte_offset(inferred_family(address)));
  return ip{encrypted_bytes};
}

auto decrypt_cryptopan(ip const& address, cryptopan_seed const& seed) -> ip {
  return decrypt_cryptopan(address, seed, inferred_family(address));
}

auto decrypt_cryptopan(ip const& address, cryptopan_seed const& seed,
                       ip::family family) -> ip {
  auto cipher = cryptopan_cipher{to_seed_bytes(seed)};
  auto decrypted_bytes
    = cipher.decrypt(to_byte_array(address), byte_offset(family));
  return ip{decrypted_bytes};
}

} // namespace tenzir
