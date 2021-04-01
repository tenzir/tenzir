//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <filesystem>

namespace fixtures {

struct filesystem {
  filesystem() {
    // Fresh afresh.
    std::filesystem::remove_all(directory);
    std::filesystem::create_directory(directory);
  }

  const std::filesystem::path directory = "vast-unit-test";
};

} // namespace fixtures
