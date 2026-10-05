//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/union_array.hpp"

#include <string_view>

namespace tenzir::nova {

/// How a definition names the types it describes.
enum class TypeNaming {
  /// The names of this data model: `int`, `uint`, and `float`.
  nova,
  /// The names of the legacy type, which every definition that leaves through
  /// the API uses: `int64`, `uint64`, and `double`.
  legacy,
};

/// Whether the elements of `list` do not share one type, so that a definition
/// with `TypeNaming::legacy` describes them as `string`.
///
/// Nulls are neutral, records unify into the union of their fields, and lists
/// unify element-wise, so only a genuine disagreement of kinds is
/// heterogeneous. Printers that serve data alongside such a definition must
/// stringify the elements of these lists, so that data and definition agree.
auto list_is_heterogeneous(RowView<List> const& list) -> bool;

/// Describes the type of a single row, in the shape of
/// `type::to_definition()`. This is what `type_of` returns and what `measure`
/// and the serve endpoints report as the definition of a schema.
///
/// A value carries neither a schema name nor attributes, so the caller
/// supplies them: `name` becomes the name of the outermost type, and
/// `internal` adds the `internal` attribute to it. Lists whose elements do not
/// share one type describe their element as a `union` with `TypeNaming::nova`,
/// and as a `string` with `TypeNaming::legacy`, which no union can pass
/// through.
auto type_definition(RowView<Data> const& row, std::string_view name = {},
                     bool internal = false,
                     TypeNaming naming = TypeNaming::legacy) -> Record;

} // namespace tenzir::nova
