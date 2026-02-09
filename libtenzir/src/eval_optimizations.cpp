//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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
    return it->second.slice(0, length);
  }
  auto result = data_to_series(x, defaults::import::table_slice_size);
  cache.insert_or_assign(x, result);
  return result.slice(0, length);
}

} // namespace tenzir
