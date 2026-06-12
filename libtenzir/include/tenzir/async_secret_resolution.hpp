//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/shared_mutex.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/ecc.hpp"
#include "tenzir/option.hpp"

#include <folly/CancellationToken.h>

namespace tenzir {

class SecretCache {
public:
  constexpr static auto ttl = std::chrono::minutes{15};

  auto lookup(std::string_view key) const -> Option<ecc::cleansing_blob>;
  auto insert(std::string key, ecc::cleansing_blob value) -> void;
  auto cleanup() -> duration;

  static auto instance() -> SharedMutex<SecretCache>&;

  static folly::CancellationSource cancel_source;

private:
  static auto start_cleanup() -> void;

  struct cache_entry {
    ecc::cleansing_blob value;
    mutable Atomic<time> last_used;
  };

  std::unordered_map<std::string, cache_entry, detail::heterogeneous_string_hash,
                     detail::heterogeneous_string_equal>
    data_;
};

} // namespace tenzir
