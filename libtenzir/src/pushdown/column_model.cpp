//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/column_model.hpp"

#include "tenzir/detail/assert.hpp"

namespace tenzir::pushdown {

auto ColumnModel::add(std::vector<std::string> path, ColumnInfo column)
  -> void {
  TENZIR_ASSERT(not path.empty());
  columns_.try_emplace(std::move(path), std::move(column));
}

auto ColumnModel::find(std::vector<std::string> const& path) const
  -> Option<ColumnInfo const&> {
  auto it = columns_.find(path);
  if (it == columns_.end()) {
    return None{};
  }
  return it->second;
}

} // namespace tenzir::pushdown
