//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/pp.hpp"

#include <filesystem>
#include <string_view>

namespace fixtures {

struct filesystem {
  explicit filesystem(std::string_view suite)
    : directory(std::filesystem::path{"tenzir-unit-test/"} / suite) {
    // Fresh afresh.
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);
  }

  const std::filesystem::path directory;
};

} // namespace fixtures
