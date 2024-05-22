//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

namespace tenzir {

/// This is meant to be used as a value type.
class session {
public:
  session(tql2::registry& reg, diagnostic_handler& dh) : reg_{reg}, dh_{dh} {
  }

  auto reg() -> tql2::registry& {
    return reg_;
  }

  auto dh() -> diagnostic_handler& {
    return dh_;
  }

  operator diagnostic_handler&() {
    return dh_;
  }

private:
  tql2::registry& reg_;
  diagnostic_handler& dh_;
};

} // namespace tenzir
