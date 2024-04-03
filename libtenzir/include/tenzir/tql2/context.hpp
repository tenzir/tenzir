//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

namespace tenzir::tql2 {

class context {
public:
  context(registry& reg, diagnostic_handler& dh) : reg_{reg}, dh_{dh} {
  }

  auto reg() -> registry& {
    return reg_;
  }

  auto dh() -> diagnostic_handler& {
    return dh_;
  }

  operator diagnostic_handler&() {
    return dh_;
  }

private:
  registry& reg_;
  diagnostic_handler& dh_;
};

} // namespace tenzir::tql2
