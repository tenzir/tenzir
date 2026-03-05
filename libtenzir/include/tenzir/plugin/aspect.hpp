//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin/base.hpp"

#include "tenzir/operator_control_plane.hpp"
#include "tenzir/table_slice.hpp"

#include <string>

namespace tenzir {

// -- aspect plugin ------------------------------------------------------------

class aspect_plugin : public virtual plugin {
public:
  /// The name of the aspect that enables `show aspect`.
  /// @note defaults to `plugin::name()`.
  virtual auto aspect_name() const -> std::string;

  /// Produces the data to show.
  virtual auto show(operator_control_plane& ctrl) const
    -> generator<table_slice>
    = 0;
};

} // namespace tenzir
