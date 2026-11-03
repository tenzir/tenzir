//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/shared_mutex.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/ecc.hpp"
#include "tenzir/option.hpp"
#include "tenzir/secret_store.hpp"

#include <folly/CancellationToken.h>

namespace tenzir {

class SecretCache {
public:
  /// Scoped cache owners run `SecretStore::run_cache_cleanup`; the process
  /// cache starts its cleanup task on first insertion.
  SecretCache() = default;

  explicit SecretCache(bool background_cleanup, duration lifetime = ttl,
                       bool refresh_on_read = true)
    : background_cleanup_{background_cleanup},
      lifetime_{lifetime},
      refresh_on_read_{refresh_on_read} {
  }

  constexpr static auto ttl = std::chrono::minutes{15};

  auto lookup(std::string_view key, time now = time::clock::now()) const
    -> Option<ecc::cleansing_blob>;
  auto insert(std::string key, ecc::cleansing_blob value,
              time now = time::clock::now()) -> void;
  auto cleanup(time now = time::clock::now()) -> duration;

  /// Invalidate outstanding resolutions as well as cached values.
  auto clear() -> void;
  auto generation() const -> uint64_t {
    return generation_;
  }

  static auto instance() -> SharedMutex<SecretCache>&;

  static folly::CancellationSource cancel_source;

private:
  static auto start_cleanup() -> void;

  bool background_cleanup_ = false;
  duration lifetime_ = ttl;
  bool refresh_on_read_ = true;
  uint64_t generation_ = 0;

  struct cache_entry {
    ecc::cleansing_blob value;
    mutable Atomic<time> last_used;
  };

  std::unordered_map<std::string, cache_entry, detail::heterogeneous_string_hash,
                     detail::heterogeneous_string_equal>
    data_;
};

/// Resolves secrets for an execution context that hosts no node.
///
/// A pipeline otherwise asks the node, which a deployment does not run. The
/// value comes back encrypted to `public_key`, as it does from the node.
class SecretStore {
public:
  explicit SecretStore(duration cache_ttl = SecretCache::ttl)
    // TODO: Switch to expiry after the last read once /secrets/stream keeps
    // cached secrets up to date. Until then, bound staleness after first use.
    : cache_{std::in_place, std::in_place, false, cache_ttl, false} {
  }

  virtual ~SecretStore() = default;

  /// Run beside the pipelines, with cancellation tied to their owner's scope.
  auto run_cache_cleanup() -> Task<void>;
  auto clear_cache() -> Task<void>;

  virtual auto resolve(std::string name, std::string public_key)
    -> Task<secret_resolution_result>
    = 0;

  /// Cached values belong to this backend, so unrelated contexts cannot reuse
  /// a value resolved under another deployment's credentials.
  auto cache() -> SharedMutex<SecretCache>& {
    return *cache_;
  }

private:
  Arc<SharedMutex<SecretCache>> cache_;
};

} // namespace tenzir
