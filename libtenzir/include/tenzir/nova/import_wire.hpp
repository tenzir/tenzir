//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/result.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace tenzir::nova {

/// Serializable envelope for crossing the importer actor's node boundary.
/// Only selected rows are sent, retaining their field order and metadata.
struct ImportWireBatch {
  std::vector<record> rows;
  std::vector<std::string> names;
  std::vector<time> import_times;
  std::vector<uint8_t> internal;

  template <class Inspector>
  friend auto inspect(Inspector& f, ImportWireBatch& x) -> bool {
    return f.object(x).fields(f.field("rows", x.rows),
                              f.field("names", x.names),
                              f.field("import_times", x.import_times),
                              f.field("internal", x.internal));
  }
};

auto to_import_wire(Events const& events) -> ImportWireBatch;
auto from_import_wire(ImportWireBatch const& batch)
  -> Result<Events, std::string>;

} // namespace tenzir::nova
