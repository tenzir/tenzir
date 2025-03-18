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

encrypted_secret_value::~encrypted_secret_value() {
  OPENSSL_cleanse(value.data(), value.size());
}

auto resolved_secret_value::clear() -> void {
  OPENSSL_cleanse(value_.data(), value_.size());
  value_.clear();
  value_.shrink_to_fit();
}

} // namespace tenzir
