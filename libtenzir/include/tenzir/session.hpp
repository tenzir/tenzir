//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/registry.hpp"

namespace tenzir {

/// This is meant to be used as a value type.
class session {
public:
  explicit session(diagnostic_handler& dh) : dh_{dh} {
  }

  auto reg() -> const registry&;

  auto dh() -> diagnostic_handler& {
    return dh_;
  }

  operator diagnostic_handler&() {
    return dh_;
  }

private:
  diagnostic_handler& dh_;
};

} // namespace tenzir
