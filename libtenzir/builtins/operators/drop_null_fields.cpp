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
      for (auto i = 1uz; i < segments.size(); ++i) {
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
        // When selectors are provided, we need to check both the selectors
        // themselves and all nested fields within them if they are records
        for (const auto& selector : selectors_) {
          // First add the selector itself
          fields_to_check.push_back(selector);
          // Then check if it's a record and add its nested fields
          auto resolved = resolve(selector, slice.schema());
          if (auto* field_offset = std::get_if<offset>(&resolved)) {
            if (! field_offset->empty()) {
              // Get the type at this offset
              auto field_type = slice.schema();
              for (const auto& idx : *field_offset) {
                if (auto* rec = try_as<record_type>(field_type)) {
                  if (idx < rec->num_fields()) {
                    field_type = rec->field(idx).type;
                  }
                }
              }
              // If the field is a record, get all its nested paths
              if (auto* record = try_as<record_type>(field_type)) {
                // Build the prefix from the selector
                auto prefix = std::vector<ast::field_path::segment>{};
                for (const auto& seg : selector.path()) {
                  prefix.push_back(seg);
                }
                auto nested_paths = get_all_field_paths(*record, prefix);
                fields_to_check.insert(fields_to_check.end(),
                                       nested_paths.begin(),
                                       nested_paths.end());
              }
            }
          }
        }
      }
      if (fields_to_check.empty()) {
        // No fields to check, yield unchanged
        co_yield std::move(slice);
        continue;
      }
      // Resolve field paths to offsets once per slice
      auto field_offsets = resolve_field_paths(fields_to_check, slice.schema());
      // Process consecutive rows by their null pattern
      auto current_start = 0uz;
      auto current_pattern = compute_null_pattern(slice, 0, field_offsets);
      for (auto row = 1uz; row < slice.rows(); ++row) {
        auto pattern = compute_null_pattern(slice, row, field_offsets);
        if (pattern != current_pattern) {
          // Pattern changed, process the current group
          auto fields_to_drop = std::vector<ast::field_path>{};
          for (auto i = 0uz; i < fields_to_check.size(); ++i) {
            if (current_pattern[i]) {
              fields_to_drop.push_back(fields_to_check[i]);
            }
          }
          auto group_slice = subslice(slice, current_start, row);
          if (fields_to_drop.empty()) {
            co_yield std::move(group_slice);
          } else {
            co_yield tenzir::drop(group_slice, fields_to_drop,
                                  ctrl.diagnostics(), false);
          }
          current_start = row;
          current_pattern = std::move(pattern);
        }
      }
      // Process the last group
      auto fields_to_drop = std::vector<ast::field_path>{};
      for (auto i = 0uz; i < fields_to_check.size(); ++i) {
        if (current_pattern[i]) {
          fields_to_drop.push_back(fields_to_check[i]);
        }
      }
      auto group_slice = subslice(slice, current_start, slice.rows());
      if (fields_to_drop.empty()) {
        co_yield std::move(group_slice);
      } else {
        co_yield tenzir::drop(group_slice, fields_to_drop, ctrl.diagnostics(),
                              false);
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
    // Special case: allow "drop_null_fields this" to behave like no arguments
    if (inv.args.size() == 1) {
      auto selector = ast::field_path::try_from(inv.args[0]);
      if (selector && selector->has_this() && selector->path().empty()) {
        // "this" with no path - treat as no arguments
        return std::make_unique<drop_null_fields_operator>(
          std::move(selectors));
      }
    }
    for (auto& arg : inv.args) {
      auto selector = ast::field_path::try_from(arg);
      if (selector) {
        if (selector->has_this()) {
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
