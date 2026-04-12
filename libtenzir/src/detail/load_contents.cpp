//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/load_contents.hpp"

#include "tenzir/error.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace tenzir::detail {

caf::expected<std::string> load_contents(const std::filesystem::path& p) {
  std::ifstream in{p};
  std::stringstream ss;
  if (not in) {
    return caf::make_error(ec::filesystem_error,
                           "failed to read from file " + p.string());
  }
  ss << in.rdbuf();
  return ss.str();
}

} // namespace tenzir::detail
