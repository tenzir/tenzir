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

// An ECC keypair. Contains public and private key as hex strings.
// Mathematically the public key is a point on the elliptic curve secp256k1
// and the private key is a random large integer.
struct string_keypair {
    std::string private_key;
    std::string public_key;

    ~string_keypair();
};

// Generate a new sec256k1 keypair.
auto generate_keypair() -> caf::expected<string_keypair>;

// Encrypt text using the given public_key
auto encrypt(std::string_view text,
             const std::string_view public_key) -> caf::expected<std::string>;

// Decrypt a ciphertext that was encrypted using the public key of `keypair`,
// using ECIES encryption with AES256-GCM cipher and HKDF as key derivation function.
auto decrypt(std::string_view ciphertext,
             const string_keypair& keypair) -> caf::expected<std::string>;

} // namespace tenzir
