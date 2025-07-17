//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/fwd.hpp>

#include <caf/expected.hpp>

namespace tenzir::ecc {

/// Cleanses memory using `OPENSSL_cleanse`
void cleanse_memory(void* start, size_t size);

/// C++ Allocator wrapper that cleanses memory on destruction
template <typename T, typename Backing = std::allocator<T>>
struct cleansing_allocator : Backing {
  auto destroy(T* ptr) noexcept(std::is_nothrow_destructible_v<T>) {
    std::allocator_traits<Backing>::destroy(*this, ptr);
    cleanse_memory(ptr, sizeof(T));
  }
};

using cleansing_string
  = std::basic_string<char, std::char_traits<char>, cleansing_allocator<char>>;

template <typename T>
using cleansing_vector = std::vector<T, cleansing_allocator<T>>;

using cleansing_blob = cleansing_vector<std::byte>;

/// Functions to perform public-key cryptography.
///
/// Under the hood they implement the ECIES protocol on the secp256k1 curve,
/// using AES-256-GCM with 16-byte tag and 16-byte nonce as cipher, and 32-byte
/// saltless HDKF for key derivation. This is, not coincidentally, the same
/// scheme that the platform uses for transmitting encrypted secrets.

// An ECC keypair. Contains public and private key as hex strings.
struct string_keypair {
  std::string private_key;
  std::string public_key;

  ~string_keypair() {
    cleanse_memory(private_key.data(), private_key.size());
    cleanse_memory(public_key.data(), public_key.size());
  }

  static auto from_private_key(std::string&& private_key)
    -> caf::expected<string_keypair>;
};

/// Generate a new keypair.
auto generate_keypair() -> caf::expected<string_keypair>;

/// Encrypt a text with the given public key.
/// The resulting ciphertext is base58-encoded, so it can be safely used in
/// any context without additional encoding or escaping.
auto encrypt(std::string_view plaintext, std::string_view public_key)
  -> caf::expected<std::string>;

/// Decrypt a ciphertext that was encrypted with the public key of `keypair`.
auto decrypt(std::string_view ciphertext, const string_keypair& keypair)
  -> caf::expected<cleansing_blob>;

/// Decrypt a ciphertext that was encrypted with the public key of `keypair`.
/// Additionally, this checks that the decrypted bytes form a valid UTF-8
/// string.
auto decrypt_string(std::string_view ciphertext, const string_keypair& keypair)
  -> caf::expected<cleansing_string>;

} // namespace tenzir::ecc
