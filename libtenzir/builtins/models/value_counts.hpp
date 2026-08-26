//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/view3.hpp>

#include <tsl/robin_map.h>

#include <limits>
#include <utility>

namespace tenzir::detail {

/// Exact value-counting state shared by the scalar statistics and the
/// persistent frequency-table model.
template <class Count>
class value_counts_state {
public:
  /// Increments `value` and returns false without mutation on overflow.
  template <class View>
  auto add(View value) -> bool {
    auto it = counts_.find(value);
    if (it == counts_.end()) {
      counts_.emplace(materialize(value), Count{1});
      return true;
    }
    if (it.value() == std::numeric_limits<Count>::max()) {
      return false;
    }
    ++it.value();
    return true;
  }

  /// Inserts restored or parsed state and rejects duplicate values.
  auto insert(data value, Count count) -> bool {
    return counts_.emplace(std::move(value), count).second;
  }

  auto clear() -> void {
    counts_.clear();
  }

  auto reserve(size_t size) -> void {
    counts_.reserve(size);
  }

  auto counts() -> tsl::robin_map<data, Count>& {
    return counts_;
  }

  auto counts() const -> tsl::robin_map<data, Count> const& {
    return counts_;
  }

private:
  tsl::robin_map<data, Count> counts_;
};

} // namespace tenzir::detail
