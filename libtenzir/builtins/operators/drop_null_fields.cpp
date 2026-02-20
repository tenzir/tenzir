//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

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

auto fields_to_check(const table_slice& slice,
                     const std::vector<ast::field_path>& selectors)
  -> std::vector<ast::field_path> {
  if (selectors.empty()) {
    if (auto* record = try_as<record_type>(slice.schema())) {
      return get_all_field_paths(*record);
    }
    return {};
  }
  auto result = std::vector<ast::field_path>{};
  for (const auto& selector : selectors) {
    // First add the selector itself.
    result.push_back(selector);
    // Then check if it's a record and add its nested fields.
    auto resolved = resolve(selector, slice.schema());
    auto* field_offset = std::get_if<offset>(&resolved);
    if (not field_offset or field_offset->empty()) {
      continue;
    }
    auto field_type = slice.schema();
    for (const auto& idx : *field_offset) {
      if (auto* rec = try_as<record_type>(field_type)) {
        if (idx < rec->num_fields()) {
          field_type = rec->field(idx).type;
        }
      }
    }
    if (auto* record = try_as<record_type>(field_type)) {
      auto prefix = std::vector<ast::field_path::segment>{};
      prefix.reserve(selector.path().size());
      for (const auto& seg : selector.path()) {
        prefix.push_back(seg);
      }
      auto nested_paths = get_all_field_paths(*record, std::move(prefix));
      result.insert(result.end(), nested_paths.begin(), nested_paths.end());
    }
  }
  return result;
}

auto fields_to_drop_for_pattern(const null_pattern& pattern,
                                const std::vector<ast::field_path>& fields)
  -> std::vector<ast::field_path> {
  TENZIR_ASSERT(pattern.size() == fields.size());
  auto result = std::vector<ast::field_path>{};
  for (auto i = 0uz; i < fields.size(); ++i) {
    if (pattern[i]) {
      result.push_back(fields[i]);
    }
  }
  return result;
}

auto drop_null_fields_impl(table_slice slice,
                           const std::vector<ast::field_path>& selectors,
                           diagnostic_handler& dh) -> std::vector<table_slice> {
  if (slice.rows() == 0) {
    return {table_slice{}};
  }
  auto fields = fields_to_check(slice, selectors);
  if (fields.empty()) {
    return {std::move(slice)};
  }
  auto field_offsets = resolve_field_paths(fields, slice.schema());
  auto result = std::vector<table_slice>{};
  auto current_start = 0uz;
  auto current_pattern = compute_null_pattern(slice, 0, field_offsets);
  for (auto row = 1uz; row < slice.rows(); ++row) {
    auto pattern = compute_null_pattern(slice, row, field_offsets);
    if (pattern == current_pattern) {
      continue;
    }
    auto fields_to_drop = fields_to_drop_for_pattern(current_pattern, fields);
    auto group_slice = subslice(slice, current_start, row);
    if (fields_to_drop.empty()) {
      result.push_back(std::move(group_slice));
    } else {
      result.push_back(tenzir::drop(group_slice, fields_to_drop, dh, false));
    }
    current_start = row;
    current_pattern = std::move(pattern);
  }
  auto fields_to_drop = fields_to_drop_for_pattern(current_pattern, fields);
  auto group_slice = subslice(slice, current_start, slice.rows());
  if (fields_to_drop.empty()) {
    result.push_back(std::move(group_slice));
  } else {
    result.push_back(tenzir::drop(group_slice, fields_to_drop, dh, false));
  }
  return result;
}

struct DropNullFieldsArgs {
  std::vector<ast::expression> fields;
};

class DropNullFields final : public Operator<table_slice, table_slice> {
public:
  explicit DropNullFields(DropNullFieldsArgs args) {
    if (args.fields.size() == 1) {
      auto selector = ast::field_path::try_from(args.fields.front());
      TENZIR_ASSERT(selector);
      if (selector->has_this() and selector->path().empty()) {
        return;
      }
    }
    selectors_.reserve(args.fields.size());
    for (auto& arg : args.fields) {
      auto selector = ast::field_path::try_from(arg);
      TENZIR_ASSERT(selector);
      TENZIR_ASSERT(not selector->has_this());
      selectors_.push_back(std::move(*selector));
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto output = drop_null_fields_impl(std::move(input), selectors_, ctx.dh());
    for (auto& slice : output) {
      co_await push(std::move(slice));
    }
  }

private:
  std::vector<ast::field_path> selectors_;
};

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
      auto output = drop_null_fields_impl(std::move(slice), selectors_,
                                          ctrl.diagnostics());
      for (auto& part : output) {
        co_yield std::move(part);
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

class plugin final : public virtual operator_plugin2<drop_null_fields_operator>,
                     public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<DropNullFieldsArgs, DropNullFields>{};
    auto fields
      = d.optional_variadic("fields", &DropNullFieldsArgs::fields, "field");
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto values = ctx.get_all(fields);
      auto locations = ctx.get_locations(fields);
      TENZIR_ASSERT(values.size() == locations.size());
      if (values.size() == 1 and values[0]) {
        auto selector = ast::field_path::try_from(*values[0]);
        if (selector and selector->has_this() and selector->path().empty()) {
          return {};
        }
      }
      for (auto i = 0uz; i < values.size(); ++i) {
        if (not values[i]) {
          diagnostic::error("expected simple selector")
            .primary(locations[i])
            .emit(ctx);
          continue;
        }
        auto selector = ast::field_path::try_from(*values[i]);
        if (not selector) {
          diagnostic::error("expected simple selector")
            .primary(locations[i])
            .emit(ctx);
          continue;
        }
        if (selector->has_this()) {
          diagnostic::error("cannot drop `this`")
            .primary(locations[i])
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }

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
