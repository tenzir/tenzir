// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/record_array.hpp"

#include <string>
#include <unordered_map>

namespace tenzir::nova {

struct NullFieldSelection {
  bool recursive = false;
  std::unordered_map<std::string, NullFieldSelection> children;

  /// Removes selected null fields on active rows. Returns whether anything
  /// changed; an unchanged record retains its original physical storage.
  auto apply(Array<Record>& record, storage::BitMap const& active) const
    -> bool;
};

} // namespace tenzir::nova
