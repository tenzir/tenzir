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

namespace tenzir {

/// The context that is available during all execution stages of a pipeline.
///
/// Right now, this just contains a diagnostic handler and the registry. In the
/// future, we can put more things here, like pipeline configuration, string and
/// type interning, etc.
class base_ctx {
public:
  base_ctx(diagnostic_handler& dh, const registry& reg) : dh_{dh}, reg_{reg} {
  }

  base_ctx(diagnostic_handler& dh, const registry& reg, caf::actor_system& sys)
    : dh_{dh}, reg_{reg}, sys_{&sys} {
  }

  // TODO: Consider using inheritance instead.
  template <class T>
    requires std::convertible_to<T, diagnostic_handler&>
             and std::convertible_to<T, const registry&>
             and std::convertible_to<T, caf::actor_system&>
  explicit(false) base_ctx(T&& x) : base_ctx{x, x, x} {
  }

  explicit(false) operator diagnostic_handler&() const {
    return dh_;
  }

  explicit(false) operator const registry&() const {
    return reg_;
  }

  auto system() -> caf::actor_system& {
    TENZIR_ASSERT(sys_);
    return *sys_;
  }

  explicit(false) operator caf::actor_system&() {
    return system();
  }

private:
  std::reference_wrapper<diagnostic_handler> dh_;
  std::reference_wrapper<const registry> reg_;
  caf::actor_system* sys_;
};

} // namespace tenzir
