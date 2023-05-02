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
#include <vast/table_slice_column.hpp>
#include <vast/type.hpp>

#include <vector>

namespace vast::plugins::tui {

/// The state of the UI.
struct ui_state {
  /// The state for a table.
  struct table_state {
    /// An extra column with row IDs.
    ftxui::Component rids;

    /// The leaf columns.
    std::vector<ftxui::Component> leaves;

    /// The slices for this table.
    std::shared_ptr<std::vector<table_slice>> slices;
  };

  /// The data to render.
  std::unordered_map<type, table_state> tables;

  /// Defines styling and colors.
  struct theme theme = default_theme;

  /// Updates the UI state when a new slice of data arrives.
  auto add(table_slice slice) -> void;
};

} // namespace vast::plugins::tui
