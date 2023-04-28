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

#include <map>
#include <string>
#include <vector>

namespace vast::plugins::tui {

/// The UI-global state.
struct ui_state {
  /// The active theme.
  struct theme theme;

  /// The buffered dataset, mapping schema names to batches.
  std::map<std::string, std::vector<table_slice>> dataset;

  /// The total number of slices in the dataset.
  std::atomic<size_t> num_slices = 0;
};

} // namespace vast::plugins::tui
