//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/ir.hpp>
#include <tenzir/nova/bitmap.hpp>

namespace parquet {
class FileMetaData;
} // namespace parquet

namespace tenzir::plugins::parquet {

/// Marks every row group of a file that may contain rows that
/// satisfy all predicates of `filter`, based on the min/max statistics and
/// null counts of its column chunks.
///
/// The decision is conservative: a row group is only ruled out if its
/// statistics prove that no row matches, so readers must still apply the
/// filter to the rows they read. Anything the statistics cannot decide keeps
/// the row group. So does a row group for which evaluating the filter may emit
/// diagnostics, to keep them independent of the statistics.
auto select_row_groups(ir::OptimizeFilter const& filter,
                       ::parquet::FileMetaData const& metadata)
  -> nova::storage::BitMap;

} // namespace tenzir::plugins::parquet
