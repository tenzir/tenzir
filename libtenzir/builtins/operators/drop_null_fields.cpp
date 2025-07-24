//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/enumerate.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

#include <arrow/type.h>
#include <fmt/format.h>

#include <bitset>

namespace tenzir::plugins::drop_null_fields {

namespace {

/// Represents which fields are null in a row
using null_pattern = std::vector<bool>;

/// Resolves field paths to offsets
auto resolve_field_paths(const std::vector<ast::field_path>& fields,
                         const type& schema) -> std::vector<offset> {
  auto result = std::vector<offset>{};
  result.reserve(fields.size());
  for (const auto& field : fields) {
    auto resolved = resolve(field, schema);
    if (auto* field_offset = std::get_if<offset>(&resolved)) {
      result.push_back(*field_offset);
    } else {
      // Field doesn't exist, use empty offset
      result.push_back(offset{});
    }
  }
  return result;
}

/// Computes the null pattern for a specific row in a table slice
auto compute_null_pattern(const table_slice& slice, size_t row_index,
                          const std::vector<offset>& field_offsets)
  -> null_pattern {
  auto pattern = null_pattern{};
  pattern.reserve(field_offsets.size());
  for (const auto& field_offset : field_offsets) {
    if (field_offset.empty()) {
      // Field doesn't exist, treat as not null
      pattern.push_back(false);
      continue;
    }
    // Navigate to the field using series constructor
    auto ser = series{slice, field_offset};
    // Check if this specific row is null
    pattern.push_back(ser.array->IsNull(row_index));
  }
  return pattern;
}

/// Groups consecutive rows with the same null pattern
struct row_group {
  size_t start;
  size_t end;
  std::vector<ast::field_path> fields_to_drop;
};

/// Finds all fields in the schema (for when no specific fields are given)
auto get_all_field_paths(const record_type& record,
                         std::vector<ast::field_path::segment> prefix = {})
  -> std::vector<ast::field_path> {
  auto result = std::vector<ast::field_path>{};
  for (const auto& field : record.fields()) {
    auto segments = prefix;
    segments.push_back({ast::identifier{field.name, location::unknown}, false});
    // Create field path expression
    ast::expression expr;
    if (segments.size() == 1) {
      expr = ast::expression{ast::root_field{segments[0].id, false}};
    } else {
      expr = ast::expression{ast::root_field{segments[0].id, false}};
      for (size_t i = 1; i < segments.size(); ++i) {
        expr = ast::expression{ast::field_access{
          std::move(expr), location::unknown, false, segments[i].id}};
      }
    }
    if (auto fp = ast::field_path::try_from(std::move(expr))) {
      result.push_back(std::move(*fp));
    }
    // Recurse for nested records
    if (auto* nested_record = try_as<record_type>(field.type)) {
      auto nested_paths = get_all_field_paths(*nested_record, segments);
      result.insert(result.end(), nested_paths.begin(), nested_paths.end());
    }
  }
  return result;
}

class drop_null_fields_operator final
  : public crtp_operator<drop_null_fields_operator> {
public:
  drop_null_fields_operator() = default;

  explicit drop_null_fields_operator(std::vector<ast::field_path> selectors)
    : selectors_{std::move(selectors)} {
  }

  auto name() const -> std::string override {
    return "tql2.drop_null_fields";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Determine which fields to check
      auto fields_to_check = std::vector<ast::field_path>{};
      if (selectors_.empty()) {
        if (auto* record = try_as<record_type>(slice.schema())) {
          fields_to_check = get_all_field_paths(*record);
        }
      } else {
        fields_to_check = selectors_;
      }
      if (fields_to_check.empty()) {
        // No fields to check, yield unchanged
        co_yield std::move(slice);
        continue;
      }
      // Resolve field paths to offsets once per slice
      auto field_offsets = resolve_field_paths(fields_to_check, slice.schema());
      // Group consecutive rows by their null pattern
      auto groups = std::vector<row_group>{};
      size_t current_start = 0;
      auto current_pattern = compute_null_pattern(slice, 0, field_offsets);
      for (size_t row = 1; row < slice.rows(); ++row) {
        auto pattern = compute_null_pattern(slice, row, field_offsets);
        if (pattern != current_pattern) {
          // Pattern changed, save the current group
          auto fields_to_drop = std::vector<ast::field_path>{};
          for (size_t i = 0; i < fields_to_check.size(); ++i) {
            if (current_pattern[i]) {
              fields_to_drop.push_back(fields_to_check[i]);
            }
          }
          groups.push_back({current_start, row, std::move(fields_to_drop)});
          current_start = row;
          current_pattern = std::move(pattern);
        }
      }
      // Add the last group
      auto fields_to_drop = std::vector<ast::field_path>{};
      for (size_t i = 0; i < fields_to_check.size(); ++i) {
        if (current_pattern[i]) {
          fields_to_drop.push_back(fields_to_check[i]);
        }
      }
      groups.push_back(
        {current_start, slice.rows(), std::move(fields_to_drop)});
      // Process each group
      for (const auto& group : groups) {
        auto group_slice = subslice(slice, group.start, group.end);
        if (group.fields_to_drop.empty()) {
          // No fields to drop in this group
          co_yield std::move(group_slice);
        } else {
          // Drop the null fields from this group
          co_yield tenzir::drop(group_slice, group.fields_to_drop,
                                ctrl.diagnostics(), false);
        }
      }
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, drop_null_fields_operator& x) -> bool {
    return f.apply(x.selectors_);
  }

private:
  std::vector<ast::field_path> selectors_;
};

} // namespace

class plugin final
  : public virtual operator_plugin2<drop_null_fields_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_("drop_null_fields");
    auto selectors = std::vector<ast::field_path>{};
    for (auto& arg : inv.args) {
      auto selector = ast::field_path::try_from(arg);
      if (selector) {
        if (selector->path().empty()) {
          diagnostic::error("cannot drop `this`").primary(*selector).emit(ctx);
          return failure::promise();
        }
        selectors.push_back(std::move(*selector));
      } else {
        diagnostic::error("expected simple selector")
          .primary(arg)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx.dh());
        return failure::promise();
      }
    }
    return std::make_unique<drop_null_fields_operator>(std::move(selectors));
  }
};

} // namespace tenzir::plugins::drop_null_fields

TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop_null_fields::plugin)
