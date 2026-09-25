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
class RowGroupMetaData;
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

/// Whether the chunk of a leaf column in a row group is dictionary-encoded and
/// holds a single valid value, going by its statistics.
///
/// Reading such a chunk as a dictionary lets the import create a constant
/// instead of one copy of the value per row. The import checks that the rows
/// really share one value, so statistics that only look constant, such as
/// truncated bounds, merely cost that check.
auto is_constant_chunk(::parquet::RowGroupMetaData const& row_group, int column)
  -> bool;

} // namespace tenzir::plugins::parquet
