//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir2/memory/forward_declarations.hpp"
#include "tenzir2/memory/generic_arrays.hpp"

#include <cstddef>

namespace tenzir2 {

/// Tracks which rows are active during expression evaluation, backed by a
/// `memory::detail::state_buffer`.
///
/// An active row is one that must be evaluated; inactive rows may be skipped
/// and their output values are unspecified. This enables short-circuit
/// evaluation of `and`, `or`, and `if`/`else` without physical slicing.
///
/// When `state_` is empty (no buffer present), every row is active,
/// regardless of `inverted_`. When `state_` is present, a row `i` is
/// inactive iff `state_.get(i) == element_state::dead` and `inverted_` is
/// false, or iff it is NOT `dead` and `inverted_` is true. The `inverted_`
/// flag lets a single buffer serve both a set of active rows and its
/// complement, e.g. an `if`/`else` branch pair built from one condition
/// buffer.
class ActiveRows {
public:
  /// All rows active.
  ActiveRows() = default;

  explicit ActiveRows(memory::detail::state_buffer state,
                      bool inverted = false)
    : state_{std::move(state)}, inverted_{inverted} {
  }

  auto is_active(std::ptrdiff_t i) const -> bool {
    if (not state_) {
      return true;
    }
    auto is_dead = state_.get(i) == memory::element_state::dead;
    return is_dead == inverted_;
  }

  auto as_constant() const -> tenzir::Option<bool> {
    if (not state_) {
      return true;
    }
    auto saw_active = false;
    auto saw_inactive = false;
    for (auto i = std::ptrdiff_t{0}; i < state_.length(); ++i) {
      (is_active(i) ? saw_active : saw_inactive) = true;
      if (saw_active and saw_inactive) {
        return tenzir::None{};
      }
    }
    return saw_active;
  }

  auto sliced(std::ptrdiff_t begin, std::ptrdiff_t end) const -> ActiveRows {
    if (not state_) {
      return *this;
    }
    return ActiveRows{state_.sliced(begin, end), inverted_};
  }

private:
  memory::detail::state_buffer state_;
  bool inverted_ = false;
};

} // namespace tenzir2
