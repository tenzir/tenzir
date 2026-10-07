//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace tenzir::nova {

struct FlattenResult {
  Array<Record> data;
  /// The renames that resolved conflicting leaf names, as `old -> new`.
  std::vector<std::string> renamed_fields;
};

/// Replaces the nested records in the records at `rows` with their leaves,
/// named by joining the field names along the path with `separator`. A list
/// that holds records or lists becomes one list per leaf, with nested lists
/// spliced into it, and an element that lacks a leaf contributes `null` to its
/// list. Leaves that share a name share a field unless they meet in a row, in
/// which case the later leaf gets the first free name of the form
/// `<name>_<n>`. Every row keeps its own field order, and leaves keep sharing
/// their storage with the input.
auto flatten(Array<Record> record, storage::BitMap const& rows,
             std::string_view separator) -> FlattenResult;

/// Splits the field names of the records at `rows` at `separator` and nests
/// the fields accordingly, also in nested records and lists. Fields that end
/// up at the same path merge if they are records; otherwise, the later field
/// of the row wins. A nested field keeps the position where its path first
/// appeared in the row. Rows at `rows` that are neither records nor lists are
/// returned unchanged. `separator` must not be empty.
auto unflatten(Array<Data> data, storage::BitMap const& rows,
               std::string_view separator) -> Array<Data>;

} // namespace tenzir::nova
