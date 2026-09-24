//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/table_description.hpp"

#include "tenzir/detail/string.hpp"

#include <clickhouse/columns/string.h>

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

auto read_description(Block const& block, diagnostic_handler& dh)
  -> failure_or<std::vector<ColumnDescription>> {
  auto failed = false;
  auto const strings = [&](std::string_view name) {
    auto result = std::shared_ptr<ColumnString>{};
    for (auto i = size_t{0}; i < block.GetColumnCount(); ++i) {
      if (block.GetColumnName(i) == name) {
        result = block[i]->As<ColumnString>();
        break;
      }
    }
    if (not result or result->Size() != block.GetRowCount()) {
      diagnostic::error("invalid ClickHouse table description")
        .note("expected String result column `{}`", name)
        .emit(dh);
      failed = true;
    }
    return result;
  };
  auto names = strings("name");
  auto types = strings("type");
  auto kinds = strings("default_type");
  auto expressions = strings("default_expression");
  auto comments = strings("comment");
  if (failed) {
    return failure::promise();
  }
  auto description = std::vector<ColumnDescription>{};
  description.reserve(block.GetRowCount());
  for (auto i = size_t{0}; i < block.GetRowCount(); ++i) {
    description.push_back({std::string{names->At(i)}, std::string{types->At(i)},
                           std::string{kinds->At(i)},
                           std::string{expressions->At(i)},
                           std::string{comments->At(i)}});
  }
  return description;
}

namespace {

// Inspect type calls recursively, never matching names inside quoted values.
auto unsupported_mapping_type(std::string_view name)
  -> Option<std::string_view> {
  for (auto kind : {"JSON", "Tuple"}) {
    if (unwrap_clickhouse_type_call(name, kind)) {
      return kind;
    }
  }
  for (auto wrapper : {"Array", "Nullable", "LowCardinality", "Map"}) {
    if (auto inner = unwrap_clickhouse_type_call(name, wrapper)) {
      for (auto argument : split_top_level_clickhouse_type_arguments(*inner)) {
        if (auto unsupported = unsupported_mapping_type(argument)) {
          return unsupported;
        }
      }
    }
  }
  return {};
}

auto mapping_path_for(ColumnDescription const& column,
                      transformer_record const& transformations,
                      diagnostic_handler& dh)
  -> failure_or<std::vector<std::string>> {
  auto type = remove_non_significant_whitespace(column.type);
  if (auto unsupported = unsupported_mapping_type(type)) {
    diagnostic::error("unsupported {} mapping for column `{}`", *unsupported,
                      column.name)
      .emit(dh);
    return failure::promise();
  }
  auto path = std::vector<std::string>{};
  for (auto component : detail::split(column.name, ".")) {
    if (component.empty()) {
      diagnostic::error("empty ClickHouse mapping path component").emit(dh);
      return failure::promise();
    }
    path.emplace_back(component);
  }
  for (auto const& [name, other] : transformations.mapping_paths) {
    auto count = std::min(path.size(), other.size());
    if (std::equal(path.begin(), path.begin() + count, other.begin())) {
      diagnostic::error("overlapping ClickHouse mapping paths `{}` and `{}`",
                        name, column.name)
        .emit(dh);
      return failure::promise();
    }
  }
  return path;
}

} // namespace

auto build_transformations(std::vector<ColumnDescription> const& description,
                           diagnostic_handler& dh)
  -> failure_or<transformer_record> {
  auto transformations = transformer_record{};
  for (auto const& column : description) {
    if (column.comment != "tenzir:catch_all") {
      continue;
    }
    if (transformations.catch_all) {
      diagnostic::error("multiple ClickHouse catch-all columns").emit(dh);
      return failure::promise();
    }
    if (column.name.find('.') != std::string::npos) {
      diagnostic::error("ClickHouse catch-all column name `{}` must not "
                        "contain dots",
                        column.name)
        .emit(dh);
      return failure::promise();
    }
    // Restricted JSON declarations can discard paths or coerce values.
    if (column.type != "JSON" or not column.default_kind.empty()) {
      diagnostic::error("invalid ClickHouse catch-all column `{}`", column.name)
        .note("expected a native JSON column without a default expression")
        .emit(dh);
      return failure::promise();
    }
    transformations.catch_all = column.name;
  }
  for (auto const& column : description) {
    auto const type_str = remove_non_significant_whitespace(column.type);
    auto const has_default = not column.default_kind.empty();
    // Keep unmarked default handling compatible. Treat EPHEMERAL as omitted
    // in marked tables so corresponding input remains in the catch-all.
    auto const generated
      = column.default_kind == "MATERIALIZED" or column.default_kind == "ALIAS"
        or (transformations.catch_all and column.default_kind == "EPHEMERAL");
    if (transformations.catch_all) {
      if (column.comment.find("tenzir:type=") != std::string::npos) {
        diagnostic::error(
          "unsupported ClickHouse type annotation on column `{}`", column.name)
          .note("type annotations are not supported in tables with a "
                "catch-all column")
          .emit(dh);
        return failure::promise();
      }
      if (not generated and column.name != *transformations.catch_all) {
        TRY(auto path, mapping_path_for(column, transformations, dh));
        transformations.mapping_paths.emplace(column.name, std::move(path));
      }
    }
    if (generated) {
      transformations.generated_columns.insert(column.name);
      continue;
    }
    auto path = path_type{column.name};
    auto collector = collecting_diagnostic_handler{};
    auto functions = make_functions_from_clickhouse(
      path, type_str, collector,
      transformations.catch_all ? MappingMode::lossless : MappingMode::legacy);
    if (not functions and has_default) {
      functions = make_default_only_transformer(type_str);
    } else {
      std::move(collector).forward_to(dh);
    }
    if (not functions) {
      return failure::promise();
    }
    functions->has_default = has_default;
    transformations.transformations.try_emplace(column.name,
                                                std::move(functions));
  }
  transformations.found_column.resize(transformations.transformations.size(),
                                      false);
  return transformations;
}

auto refresh_transformations(transformer_record& transformations,
                             CachedDescription& cached,
                             std::vector<ColumnDescription> description,
                             std::chrono::steady_clock::time_point now,
                             diagnostic_handler& dh) -> failure_or<void> {
  if (cached.columns == description) {
    cached.refreshed = now;
    return {};
  }
  TRY(auto replacement, build_transformations(description, dh));
  if (transformations.catch_all and not replacement.catch_all) {
    diagnostic::error("ClickHouse catch-all marker was removed").emit(dh);
    return failure::promise();
  }
  transformations = std::move(replacement);
  cached = {std::move(description), now};
  return {};
}

} // namespace tenzir::plugins::clickhouse
