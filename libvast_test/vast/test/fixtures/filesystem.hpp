// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/test/test.hpp"

#include "vast/error.hpp"
#include "vast/path.hpp"

#include <filesystem>

namespace fixtures {

struct filesystem {
  filesystem() {
    // Fresh afresh.
    std::filesystem::remove_all(std::filesystem::path{directory.str()});
    if (auto err = mkdir(directory))
      // error is non-recoverable, so we just abort
      FAIL(vast::render(err));
  }

  vast::path directory = "vast-unit-test";
};

} // namespace fixtures
