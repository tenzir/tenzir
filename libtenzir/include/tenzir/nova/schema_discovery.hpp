//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/result.hpp"
#include "tenzir/type.hpp"

#include <map>
#include <string>
#include <vector>

namespace tenzir::nova {

/// Merges two types that differ only where one side is `null`, including
/// inside lists and records. Fails for any other difference, such as distinct
/// concrete types, record field sets, or field order.
auto refine_null_types(type const& lhs, type const& rhs)
  -> Result<type, std::string>;

/// Discovers types from columns, constructing each distinct type once. Rows
/// only carry type IDs; scalar values never participate in discovery.
///
/// The discovered types carry no names or attributes. ID 0 is always `null`.
/// Discovery fails for secrets and for lists whose elements have no common
/// type.
class SchemaDiscovery {
public:
  SchemaDiscovery();

  /// Returns the interned types, indexed by the IDs returned from `discover`.
  auto types() const -> std::vector<type> const&;

  /// Returns one type ID per row of `array`. Rows outside `selected` map to 0.
  auto discover(Array<Data> const& array, storage::BitMap const& selected)
    -> Result<std::vector<size_t>, std::string>;

private:
  auto intern(type value) -> size_t;
  auto
  discover_record(Array<Record> const& array, storage::BitMap const& selected)
    -> Result<std::vector<size_t>, std::string>;
  auto discover_list(Array<List> const& array, storage::BitMap const& selected)
    -> Result<std::vector<size_t>, std::string>;

  std::vector<type> types_;
  std::map<type, size_t> ids_;
};

/// The active rows of a batch of events that share one record type.
struct SchemaGroup {
  /// The unnamed record type of the rows.
  type schema;
  /// The rows of the group, in ascending order.
  std::vector<storage::Index> rows;
};

struct SchemaGroupingOptions {
  /// Whether to merge groups whose types differ only where one of them is
  /// `null`, assigning each row to the first compatible group. Without it,
  /// rows are grouped by their exact structural type.
  bool refine_nulls = false;
};

/// Groups the active rows of `events` by their discovered record type, without
/// materializing rows. Event metadata does not participate in grouping. Groups
/// are ordered by their first row, so they follow the arrival order of the
/// events. Each group can be passed to `to_arrow_record_batch` directly.
auto group_by_schema(Events const& events, SchemaGroupingOptions options = {})
  -> Result<std::vector<SchemaGroup>, std::string>;

} // namespace tenzir::nova
