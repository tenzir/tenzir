//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#pragma once

#include "tenzir/nova/events.hpp"
#include "tenzir/result.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace tenzir {

/// Serializable envelope for crossing the importer actor's node boundary.
/// The payload uses the existing columnar Bitz v2 codec.
struct ImportWireBatch {
  std::vector<std::byte> payload;

  template <class Inspector>
  friend auto inspect(Inspector& f, ImportWireBatch& x) -> bool {
    return f.object(x).fields(f.field("payload", x.payload));
  }
};

auto to_import_wire(nova::Events const& events)
  -> Result<ImportWireBatch, std::string>;
auto from_import_wire(ImportWireBatch const& batch)
  -> Result<nova::Events, std::string>;

} // namespace tenzir
