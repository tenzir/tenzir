//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution.hpp"

#include <openssl/crypto.h>

namespace tenzir {

auto resolved_secret_value::clear() -> void {
  match(value_, [](auto& value) {
    ecc::cleanse_memory(value.data(), value.size());
    value.clear();
    value.shrink_to_fit();
  });
}

} // namespace tenzir
