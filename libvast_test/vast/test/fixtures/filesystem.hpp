//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/pp.hpp"
#include "vast/logger.hpp"

#include <filesystem>

namespace fixtures {

struct filesystem {
  filesystem(const std::string& suite) : directory("vast-unit-test/" + suite) {
    // Fresh afresh.
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);
  }

  const std::filesystem::path directory;
};

} // namespace fixtures
