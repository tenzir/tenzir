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

/// The operator configuration.
struct printer_args {
  bool real_time;
  bool hide_types;

  friend auto inspect(auto& f, printer_args& x) -> bool {
    return f.object(x)
      .pretty_name("printer_args")
      .fields(f.field("real-time", x.real_time),
              f.field("hide_types", x.hide_types));
  }
};

} // namespace tenzir::plugins::explore
