//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/ir.hpp>
#include <tenzir/option.hpp>

#include <vector>

namespace parquet {
class FileMetaData;
} // namespace parquet

namespace tenzir::plugins::parquet {

/// The leaf columns of a file that a reader must decode to produce the fields
/// in `projection`, in ascending order, or all of them for `None`.
///
/// A path selects every leaf below the field it names, so a record selects
/// all of its fields. Only records decode field by field: lists, maps, and
/// extension types such as subnets are read whole. A path whose first field
/// the file lacks selects nothing, and one that leaves the file further down
/// reads the last record on its way whole.
auto select_columns(::parquet::FileMetaData const& metadata,
                    Option<ir::OptimizeProjection> const& projection)
  -> std::vector<int>;

} // namespace tenzir::plugins::parquet
