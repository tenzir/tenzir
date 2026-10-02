//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/create_table.hpp"

#include "tenzir/nova/array.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"

#include <fmt/format.h>

#include <algorithm>

namespace tenzir::plugins::clickhouse {

namespace {

using nova::storage::BitMap;
using nova::storage::Index;

/// Values of one field in one batch.
struct Slot {
  nova::Array<nova::Data> data;
  /// The rows that have the field.
  BitMap rows;
};

/// The values of one field across all batches.
using Slots = std::vector<Slot>;

class Inference {
public:
  Inference(TableOptions const& options, diagnostic_handler& dh)
    : options_{options},
      dh_{dh},
      json_seen_(options.json.size(), false),
      low_cardinality_seen_(options.low_cardinality.size(), false) {
  }

  /// Returns the column definitions of the record fields in `slots`.
  auto fields(Slots const& slots, bool top_level)
    -> failure_or<std::vector<std::string>> {
    auto names = std::vector<std::string>{};
    auto children = std::vector<Slots>{};
    for (auto const& slot : slots) {
      auto records = slot.data.get_alternative<nova::Record>();
      if (not records) {
        continue;
      }
      auto rows = records->present & slot.rows;
      if (not rows.any()) {
        continue;
      }
      auto const primary = records->data.to_primary();
      auto const& storage
        = *as<nova::storage::RecordStorage>(primary.storage());
      for (auto i = size_t{0}; i < storage.names_by_index.size(); ++i) {
        auto present = storage.arrays[i].present & rows;
        if (not present.any()) {
          continue;
        }
        auto const name = storage.names_by_index[i];
        auto it = std::ranges::find(names, name);
        if (it == names.end()) {
          names.emplace_back(name);
          children.emplace_back();
          it = names.end() - 1;
        }
        children[it - names.begin()].push_back(
          {storage.arrays[i].data, std::move(present)});
      }
    }
    auto result = std::vector<std::string>{};
    for (auto i = size_t{0}; i < names.size(); ++i) {
      auto const is_primary
        = top_level and options_.primary and names[i] == primary_name();
      path_.push_back(names[i]);
      auto type = infer(children[i], not is_primary);
      path_.pop_back();
      if (not type) {
        return failure::promise();
      }
      result.push_back(
        fmt::format("{} {}", quote_identifier_component(names[i]), *type));
    }
    if (top_level) {
      // Primary keys must not be `JSON`, which is validated earlier.
      primary_found_
        = options_.primary
          and std::ranges::find(names, primary_name()) != names.end();
    }
    return result;
  }

  auto primary_name() const -> std::string {
    auto const& name = options_.primary->inner;
    return table_name_quoting.is_quoted(name)
             ? unquote_identifier_component(name)
             : name;
  }

  auto primary_found() const -> bool {
    return primary_found_;
  }

  auto json_seen(size_t index) const -> bool {
    return json_seen_[index];
  }

  auto low_cardinality_seen(size_t index) const -> bool {
    return low_cardinality_seen_[index];
  }

private:
  auto path() const -> std::string {
    return fmt::format("{}", fmt::join(path_, "."));
  }

  auto infer(Slots const& slots, bool nullable) -> failure_or<std::string> {
    if (auto index = index_of(options_.json, path_)) {
      json_seen_[*index] = true;
      return std::string{"JSON"};
    }
    auto kinds = std::vector<std::string_view>{};
    auto check = [&]<class Tag>() {
      for (auto const& slot : slots) {
        auto values = slot.data.get_alternative<Tag>();
        if (values and (values->present & slot.rows).any()) {
          kinds.push_back(nova::Type<Tag>::static_name);
          return;
        }
      }
    };
    [&]<class... Tags>(nova::TypeList<Tags...>) {
      (check.template operator()<Tags>(), ...);
    }(nova::TypeList<nova::Bool, nova::Int, nova::UInt, nova::Float,
                     nova::String, nova::Blob, nova::Secret, nova::Ip,
                     nova::Subnet, nova::Time, nova::Duration, nova::List,
                     nova::Record>{});
    if (kinds.empty()) {
      diagnostic::error("column `{}` only contains `null`", path())
        .note("its type cannot be inferred when creating the table")
        .hint("make sure that the first events have a value for it")
        .emit(dh_);
      return failure::promise();
    }
    if (kinds.size() > 1) {
      diagnostic::error("column `{}` has conflicting types `{}`", path(),
                        fmt::join(kinds, "`, `"))
        .note("a column must have a single type when creating the table")
        .emit(dh_);
      return failure::promise();
    }
    auto const kind = kinds.front();
    auto nullable_type = [&](std::string_view name) {
      return nullable ? fmt::format("Nullable({})", name) : std::string{name};
    };
    auto result = std::string{};
    if (kind == "bool") {
      result = nullable_type("Bool");
    } else if (kind == "int" or kind == "duration") {
      result = nullable_type("Int64");
    } else if (kind == "uint") {
      result = nullable_type("UInt64");
    } else if (kind == "float") {
      result = nullable_type("Float64");
    } else if (kind == "string") {
      result = nullable_type("String");
    } else if (kind == "ip") {
      result = nullable_type("IPv6");
    } else if (kind == "time") {
      result = nullable_type("DateTime64(9)");
    } else if (kind == "subnet") {
      result = nullable ? "Tuple(ip Nullable(IPv6),length Nullable(UInt8))"
                        : "Tuple(ip IPv6,length UInt8)";
    } else if (kind == "blob") {
      result = "Array(UInt8)";
    } else if (kind == "secret") {
      diagnostic::error("column `{}` has type `secret`", path())
        .note("secrets cannot be sent to ClickHouse")
        .emit(dh_);
      return failure::promise();
    } else if (kind == "record") {
      TRY(auto elements, fields(slots, false));
      if (elements.empty()) {
        diagnostic::error("column `{}` is an empty record, which is not "
                          "supported",
                          path())
          .note("empty `Tuple`s cannot be send to ClickHouse")
          .emit(dh_);
        return failure::promise();
      }
      result = fmt::format("Tuple({})", fmt::join(elements, ", "));
    } else {
      TENZIR_ASSERT_EQ(kind, "list");
      TRY(auto elements, infer(list_elements(slots), nullable));
      result = fmt::format("Array({})", elements);
    }
    if (auto index = index_of(options_.low_cardinality, path_)) {
      low_cardinality_seen_[*index] = true;
      if (not is_lowcardinality_supported_inner(result)) {
        diagnostic::error("column `{}` cannot be a `LowCardinality` column",
                          path())
          .primary(options_.low_cardinality[*index])
          .note("`LowCardinality` is only supported for `string` columns, but "
                "`{}` has the ClickHouse type `{}`",
                path(), result)
          .emit(dh_);
        return failure::promise();
      }
      result = fmt::format("LowCardinality({})", result);
    }
    return result;
  }

  /// The elements of the lists in `slots`.
  static auto list_elements(Slots const& slots) -> Slots {
    using Constant
      = nova::storage::ConstantStorage<nova::List, nova::RowView<nova::List>>;
    auto result = Slots{};
    for (auto const& slot : slots) {
      auto lists = slot.data.get_alternative<nova::List>();
      if (not lists) {
        continue;
      }
      auto const rows = lists->present & slot.rows;
      if (not rows.any()) {
        continue;
      }
      auto const* constant = try_as<Constant>(lists->data.storage());
      auto primary = constant
                       ? nova::Array<nova::List>{Constant{1, constant->value()}}
                           .to_primary()
                       : lists->data.to_primary();
      auto const& storage = as<nova::storage::ListStorage>(primary.storage());
      auto elements = BitMap::Mutable{storage.values().length()};
      for (auto row : nova::storage::true_bits(rows)) {
        auto const span = storage.spans()[constant ? 0 : row];
        for (auto i = span.begin; i < span.end; ++i) {
          std::ignore = elements.set(i, true);
        }
      }
      result.push_back({storage.values(), std::move(elements).finish()});
    }
    return result;
  }

  TableOptions const& options_;
  diagnostic_handler& dh_;
  std::vector<std::string_view> path_;
  std::vector<char> json_seen_;
  std::vector<char> low_cardinality_seen_;
  bool primary_found_ = false;
};

} // namespace

auto make_create_table_query(std::string_view table,
                             std::span<nova::Events const> events,
                             TableOptions const& options,
                             diagnostic_handler& dh)
  -> failure_or<std::string> {
  TENZIR_ASSERT(options.primary);
  auto slots = Slots{};
  for (auto const& batch : events) {
    slots.push_back({nova::Array<nova::Data>{batch.data}, batch.mask});
  }
  auto inference = Inference{options, dh};
  TRY(auto columns, inference.fields(slots, true));
  // Missing `json` paths can only be created at the top level.
  for (auto i = size_t{0}; i < options.json.size(); ++i) {
    if (inference.json_seen(i)) {
      continue;
    }
    auto const& path = options.json[i];
    if (path.inner.size() != 1) {
      diagnostic::error("`json` column `{}` is missing from the input",
                        to_dotted_string(path.inner))
        .primary(path)
        .note("a nested column's parent record must be present when "
              "creating the table")
        .emit(dh);
      return failure::promise();
    }
    columns.push_back(
      fmt::format("{} JSON", quote_identifier_component(path.inner.front())));
  }
  for (auto i = size_t{0}; i < options.low_cardinality.size(); ++i) {
    if (inference.low_cardinality_seen(i)) {
      continue;
    }
    auto const& path = options.low_cardinality[i];
    diagnostic::error("`low_cardinality` column `{}` is missing from the input",
                      to_dotted_string(path.inner))
      .primary(path)
      .note("the column's inner type cannot be inferred when creating the "
            "table")
      .emit(dh);
    return failure::promise();
  }
  if (not inference.primary_found()) {
    diagnostic::error(
      "cannot create table: primary key does not exist in input")
      .primary(*options.primary, "column `{}` does not exist",
               options.primary->inner)
      .emit(dh);
    return failure::promise();
  }
  auto const modifier
    = options.mode.inner == mode::create_append ? "IF NOT EXISTS " : "";
  return fmt::format("CREATE TABLE {}{} ({}) ENGINE = MergeTree ORDER BY {}",
                     modifier, table, fmt::join(columns, ", "),
                     quote_identifier_component(inference.primary_name()));
}

} // namespace tenzir::plugins::clickhouse
