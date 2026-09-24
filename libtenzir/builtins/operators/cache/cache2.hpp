//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async/fwd.hpp>
#include <tenzir/location.hpp>
#include <tenzir/option.hpp>
#include <tenzir/time.hpp>

#include <cstdint>
#include <string>

namespace tenzir::plugins::cache {

struct CacheArgs {
  std::string id;
  Option<located<uint64_t>> capacity;
  Option<located<duration>> read_timeout;
  Option<located<duration>> write_timeout;

  friend auto inspect(auto& f, CacheArgs& x) -> bool {
    return f.object(x).fields(f.field("id", x.id),
                              f.field("capacity", x.capacity),
                              f.field("read_timeout", x.read_timeout),
                              f.field("write_timeout", x.write_timeout));
  }
};

namespace cache2 {

/// Configure the process-local Nova cache store.
auto configure(uint64_t max_bytes) -> void;

/// Shut down the process-local Nova cache store.
auto shutdown() -> void;

/// Expire caches whose read or write deadline elapsed.
auto expire() -> void;

auto make_write(CacheArgs args) -> AnyOperator;
auto make_read(CacheArgs args) -> AnyOperator;
auto make_readwrite(CacheArgs args) -> AnyOperator;

} // namespace cache2

} // namespace tenzir::plugins::cache
