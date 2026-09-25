//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#pragma once

#include "tenzir/table_slice.hpp"
#include "tenzir/uuid.hpp"

#include <vector>

namespace tenzir {

/// An importer snapshot whose publication barrier remains held until the
/// catalog candidate lookup has completed.
struct recent_snapshot {
  std::vector<table_slice> events;
  uuid barrier;

  template <class Inspector>
  friend auto inspect(Inspector& f, recent_snapshot& x) -> bool {
    return f.object(x).fields(f.field("events", x.events),
                              f.field("barrier", x.barrier));
  }
};

} // namespace tenzir
