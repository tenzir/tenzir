//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/column_selection.hpp"

#include <tenzir/tql2/ast.hpp>

#include <arrow/type.h>
#include <parquet/arrow/schema.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>

#include <algorithm>
#include <numeric>
#include <span>

namespace tenzir::plugins::parquet {

namespace {

using ::parquet::arrow::SchemaField;

/// Adds every leaf column below `field`.
auto add_leaves(SchemaField const& field, std::vector<int>& columns) -> void {
  if (field.is_leaf()) {
    columns.push_back(field.column_index);
    return;
  }
  for (auto const& child : field.children) {
    add_leaves(child, columns);
  }
}

/// Adds the leaf columns below `field` that the rest of a path needs.
auto add_path(SchemaField const& field,
              std::span<ast::field_path::segment const> path,
              std::vector<int>& columns) -> void {
  // The restored type decides, not the Parquet schema: a subnet, for example,
  // is stored like a record.
  if (path.empty() or field.field->type()->id() != arrow::Type::STRUCT) {
    add_leaves(field, columns);
    return;
  }
  auto found = false;
  for (auto const& child : field.children) {
    // The import keeps the last of duplicate fields, so read all of them.
    if (child.field->name() == path.front().id.name) {
      found = true;
      add_path(child, path.subspan(1), columns);
    }
  }
  if (not found) {
    // Keep the record as it was read before, so that evaluating the path
    // reports the missing field the same way.
    add_leaves(field, columns);
  }
}

} // namespace

auto select_columns(::parquet::FileMetaData const& metadata,
                    Option<ir::OptimizeProjection> const& projection)
  -> std::vector<int> {
  auto all = [&] {
    auto columns = std::vector<int>(metadata.num_columns());
    std::iota(columns.begin(), columns.end(), 0);
    return columns;
  };
  if (not projection
      or std::ranges::any_of(*projection, [](ast::field_path const& path) {
           return path.path().empty();
         })) {
    return all();
  }
  // The manifest carries the types that the reader restores for the columns,
  // including those from an embedded Arrow schema.
  auto manifest = ::parquet::arrow::SchemaManifest{};
  if (not ::parquet::arrow::SchemaManifest::Make(
            metadata.schema(), metadata.key_value_metadata(),
            ::parquet::ArrowReaderProperties{}, &manifest)
            .ok()) {
    return all();
  }
  auto columns = std::vector<int>{};
  for (auto const& path : *projection) {
    // An event that lacks a field reports it without any of the file's data.
    for (auto const& field : manifest.schema_fields) {
      if (field.field->name() == path.path().front().id.name) {
        add_path(field, path.path().subspan(1), columns);
      }
    }
  }
  std::ranges::sort(columns);
  auto duplicates = std::ranges::unique(columns);
  columns.erase(duplicates.begin(), duplicates.end());
  return columns;
}

} // namespace tenzir::plugins::parquet
