//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/location.hpp>

#include <optional>
#include <string>

namespace tenzir::plugins::explore {

/// The plugin configuration.
struct operator_args {
  std::optional<located<int>> width;
  std::optional<located<int>> height;
  std::optional<location> fullscreen;
  std::optional<location> hide_types;
  std::optional<located<std::string>> navigator_position;
  std::optional<location> navigator_auto_hide;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("width", x.width), f.field("height", x.height),
              f.field("fullscreen", x.fullscreen),
              f.field("hide_types", x.hide_types),
              f.field("navigator_position", x.navigator_position),
              f.field("navigator_auto_hide", x.navigator_auto_hide));
  }
};

} // namespace tenzir::plugins::explore
