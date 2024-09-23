//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/operator_control_plane.hpp"

namespace tenzir {

class exec_ctx {
public:
  // TODO: Reconsider constructor and conversions.
  exec_ctx(operator_control_plane& ctrl) : ctrl_{ctrl} {
  }

  auto ctrl() -> operator_control_plane& {
    return ctrl_;
  }

  auto dh() -> diagnostic_handler& {
    return ctrl_.diagnostics();
  }

  operator operator_control_plane&() {
    return ctrl();
  }

  operator diagnostic_handler&() {
    return dh();
  }

private:
  operator_control_plane& ctrl_;
};

} // namespace tenzir
