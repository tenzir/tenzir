//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tui/theme.hpp"

#include <vast/table_slice.hpp>

#include <vector>

namespace vast::plugins::tui {

/// The UI-global state.
struct ui_state {
  /// The active theme.
  struct theme theme;

  /// The data to render.
  std::vector<table_slice> data;
};

} // namespace vast::plugins::tui
