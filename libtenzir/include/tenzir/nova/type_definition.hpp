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

/// Describes the type of a single row, in the shape of
/// `type::to_definition()`. This is what `type_of` returns and what `measure`
/// and the serve endpoints report as the definition of a schema.
///
/// A value carries neither a schema name nor attributes, so the caller
/// supplies them: `name` becomes the name of the outermost type, and
/// `internal` adds the `internal` attribute to it. Lists whose elements do not
/// share one type describe their element as a `union`, which no legacy type
/// can express.
auto type_definition(RowView<Data> const& row, std::string_view name = {},
                     bool internal = false,
                     TypeNaming naming = TypeNaming::legacy) -> Record;

/// Like `type_definition()`, but in the shape of
/// `type::to_legacy_definition()`, which the serve endpoints report by
/// default. A type is always named the legacy way here.
auto legacy_type_definition(RowView<Data> const& row,
                            std::string_view name = {}, bool internal = false)
  -> Record;

} // namespace tenzir::nova
