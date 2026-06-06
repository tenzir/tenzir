//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ip.hpp"

#include <array>
#include <cstddef>

namespace tenzir {

/// The Crypto-PAn seed size in bytes.
inline constexpr auto cryptopan_seed_size = size_t{32};

/// The 256-bit seed for the Crypto-PAn cipher and padding.
using cryptopan_seed = std::array<std::byte, cryptopan_seed_size>;

/// Encrypts an IP address using the Crypto-PAn algorithm.
auto encrypt_cryptopan(ip const& address, cryptopan_seed const& seed) -> ip;

/// Decrypts an IP address using the Crypto-PAn algorithm.
auto decrypt_cryptopan(ip const& address, cryptopan_seed const& seed) -> ip;

/// Decrypts an IP address using the Crypto-PAn algorithm.
auto decrypt_cryptopan(ip const& address, cryptopan_seed const& seed,
                       ip::family family) -> ip;

} // namespace tenzir
