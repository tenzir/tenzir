//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#pragma once

#include "tenzir/partition_synopsis.hpp"

#include <caf/error.hpp>

#include <vector>

namespace tenzir {

struct NovaPersistResult {
  struct Output {
    tenzir::uuid uuid;
    partition_synopsis_ptr synopsis;
    caf::error failure = caf::none;
  };

  std::vector<Output> outputs;
};

} // namespace tenzir
