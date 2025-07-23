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
#include <unordered_map>

namespace tenzir::plugins::drop_nulls {

namespace {

/// Represents which fields are null in a row
using null_pattern = std::vector<bool>;

/// Computes the null pattern for a specific row in a table slice
auto compute_null_pattern(const table_slice& slice, size_t row_index,
                          const std::vector<ast::field_path>& fields_to_check)
  -> null_pattern {
  auto pattern = null_pattern{};
  pattern.reserve(fields_to_check.size());
  const auto& batch = to_record_batch(slice);
  for (const auto& field : fields_to_check) {
    // Resolve the field path to get the column
    auto resolved = resolve(field, slice.schema());
    if (auto* field_offset = std::get_if<offset>(&resolved)) {
      // Navigate to the column using the offset
      const auto* array = batch->column((*field_offset)[0]).get();
      // For nested fields, navigate deeper
      bool navigation_failed = false;
      for (size_t i = 1; i < field_offset->size(); ++i) {
        if (auto* struct_array
            = dynamic_cast<const arrow::StructArray*>(array)) {
          array = struct_array->field((*field_offset)[i]).get();
        } else {
          // Can't navigate deeper, treat as not null
          pattern.push_back(false);
          navigation_failed = true;
          break;
        }
      }
      if (! navigation_failed) {
        // Check if this specific row is null
        pattern.push_back(array->IsNull(row_index));
      }
    } else {
      // Field doesn't exist, treat as not null
      pattern.push_back(false);
    }
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
auto get_all_field_paths(const type& schema,
                         std::vector<ast::field_path::segment> prefix = {})
  -> std::vector<ast::field_path> {
  auto result = std::vector<ast::field_path>{};
  const auto* record = try_as<record_type>(schema);
  if (! record) {
    return result;
  }
  for (const auto& field : record->fields()) {
    auto segments = prefix;
    segments.push_back({ast::identifier{field.name, location::unknown}, false});
    // Create field path expression
    ast::expression expr;
    if (segments.size() == 1) {
      expr = ast::expression{
        ast::root_field{segments[0].id, segments[0].has_question_mark}};
    } else {
      expr = ast::expression{
        ast::root_field{segments[0].id, segments[0].has_question_mark}};
      for (size_t i = 1; i < segments.size(); ++i) {
        expr = ast::expression{
          ast::field_access{std::move(expr), location::unknown,
                            segments[i].has_question_mark, segments[i].id}};
      }
    }
    if (auto fp = ast::field_path::try_from(std::move(expr))) {
      result.push_back(std::move(*fp));
    }
    // Recurse for nested records
    if (try_as<record_type>(field.type)) {
      auto nested_paths = get_all_field_paths(field.type, segments);
      result.insert(result.end(), nested_paths.begin(), nested_paths.end());
    }
  }
  return result;
}

class drop_nulls_operator final : public crtp_operator<drop_nulls_operator> {
public:
  drop_nulls_operator() = default;

  explicit drop_nulls_operator(std::vector<ast::field_path> selectors)
    : selectors_{std::move(selectors)} {
  }

  auto name() const -> std::string override {
    return "tql2.drop_nulls";
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
      auto fields_to_check
        = selectors_.empty() ? get_all_field_paths(slice.schema()) : selectors_;
      if (fields_to_check.empty()) {
        // No fields to check, yield unchanged
        co_yield std::move(slice);
        continue;
      }
      // Group consecutive rows by their null pattern
      auto groups = std::vector<row_group>{};
      size_t current_start = 0;
      auto current_pattern = compute_null_pattern(slice, 0, fields_to_check);
      for (size_t row = 1; row < slice.rows(); ++row) {
        auto pattern = compute_null_pattern(slice, row, fields_to_check);
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

  friend auto inspect(auto& f, drop_nulls_operator& x) -> bool {
    return f.apply(x.selectors_);
  }

private:
  std::vector<ast::field_path> selectors_;
};

class plugin final : public virtual operator_plugin2<drop_nulls_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_("drop_nulls");
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
    return std::make_unique<drop_nulls_operator>(std::move(selectors));
  }
};

} // namespace

} // namespace tenzir::plugins::drop_nulls

TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop_nulls::plugin)