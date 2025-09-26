//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/registry.hpp"

#include <memory>

namespace tenzir {

/// The context that is available during all execution stages of a pipeline.
///
/// Right now, this just contains a diagnostic handler and the registry. In the
/// future, we can put more things here, like pipeline configuration, string and
/// type interning, etc.
class base_ctx {
public:
  base_ctx(diagnostic_handler& dh, const registry& reg)
    : dh_{dh}, reg_ptr_{&reg} {
  }

  // Own the registry via shared_ptr, useful when using snapshot semantics.
  base_ctx(diagnostic_handler& dh, std::shared_ptr<const registry> reg)
    : dh_{dh}, reg_holder_{std::move(reg)} {
    reg_ptr_ = reg_holder_.get();
  }

  // TODO: Consider using inheritance instead.
  template <class T>
    requires std::convertible_to<T, diagnostic_handler&>
             and std::convertible_to<T, const registry&>
  explicit(false) base_ctx(T&& x) : base_ctx{x, x} {
  }

  explicit(false) operator diagnostic_handler&() {
    return dh_;
  }

  explicit(false) operator const registry&() {
    TENZIR_ASSERT(reg_ptr_);
    return *reg_ptr_;
  }

private:
  std::reference_wrapper<diagnostic_handler> dh_;
  const registry* reg_ptr_ = nullptr;
  std::shared_ptr<const registry> reg_holder_{};
};

} // namespace tenzir
