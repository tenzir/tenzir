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

namespace tenzir::nova {

struct ImportShapeKey {
  std::string name;
  bool internal = false;
  std::vector<std::string> fields;

  friend auto operator==(ImportShapeKey const&, ImportShapeKey const&) -> bool
    = default;
};

struct ImportShapeKeyHash {
  auto operator()(ImportShapeKey const& key) const -> size_t;
};

struct ImportShapeGroup {
  ImportShapeKey key;
  storage::BitMap mask;
};

/// Resolves batch-local shape IDs to ordered field names and groups selected
/// rows by shape and metadata without copying their values.
auto group_import_shapes(Events const& events)
  -> Result<std::vector<ImportShapeGroup>, std::string>;

} // namespace tenzir::nova
