//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/eval_optimizations.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/tql2/ast.hpp"

#include <folly/concurrency/ConcurrentHashMap.h>

namespace tenzir {

namespace {

auto& the_eval_cache() {
  static auto instance = folly::ConcurrentHashMap<data, series>{};
  return instance;
}

} // namespace

void clear_eval_cache() {
  the_eval_cache().clear();
}

auto cached_data_to_series(const data& x, int64_t length) -> series {
  auto& cache = the_eval_cache();
  auto it = cache.find(x);
  if (it != cache.end()) {
    if (it->second.length() >= length) {
      return it->second.slice(0, length);
    }
  }
  auto const size = std::max(defaults::import::table_slice_size,
                             detail::narrow<std::uint64_t>(length));
  auto result = data_to_series(x, size);
  cache.insert_or_assign(x, result);
  return result.slice(0, length);
}

} // namespace tenzir
