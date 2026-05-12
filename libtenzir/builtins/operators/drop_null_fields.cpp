//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_vector.h>
#include <arrow/datum.h>

#include <algorithm>
#include <span>
#include <unordered_map>

namespace tenzir::plugins::drop_null_fields {

namespace {

using null_pattern = std::vector<uint64_t>;

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

struct null_accessor {
  std::shared_ptr<arrow::Array> array;
  bool exists = false;
};

struct null_pattern_hash {
  auto operator()(null_pattern const& pattern) const noexcept -> size_t {
    return tenzir::hash(pattern);
  }
};

struct bucket {
  null_pattern pattern;
  std::vector<int64_t> rows;
};

auto build_null_accessors(const table_slice& slice,
                          std::span<const offset> field_offsets)
  -> std::vector<null_accessor> {
  auto result = std::vector<null_accessor>{};
  result.reserve(field_offsets.size());
  for (auto const& field_offset : field_offsets) {
    if (field_offset.empty()) {
      result.push_back({});
      continue;
    }
    result.push_back({
      .array = field_offset.get(slice).second,
      .exists = true,
    });
  }
  return result;
}

auto compute_null_pattern(std::span<const null_accessor> accessors, size_t row,
                          null_pattern& pattern) -> void {
  std::ranges::fill(pattern, uint64_t{0});
  for (auto i = 0uz; i < accessors.size(); ++i) {
    auto const& accessor = accessors[i];
    if (not accessor.exists or not accessor.array->IsNull(row)) {
      continue;
    }
    pattern[i / 64] |= uint64_t{1} << (i % 64);
  }
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
  TENZIR_ASSERT(pattern.size() >= (fields.size() + 63) / 64);
  auto result = std::vector<ast::field_path>{};
  for (auto i = 0uz; i < fields.size(); ++i) {
    auto const word = pattern[i / 64];
    auto const bit = uint64_t{1} << (i % 64);
    if (word & bit) {
      result.push_back(fields[i]);
    }
  }
  return result;
}

auto take_rows(const table_slice& slice, std::span<const int64_t> rows)
  -> table_slice {
  auto builder = arrow::Int64Builder{tenzir::arrow_memory_pool()};
  check(builder.Reserve(detail::narrow<int64_t>(rows.size())));
  for (auto row : rows) {
    builder.UnsafeAppend(row);
  }
  auto batch
    = check(arrow::compute::Take(to_record_batch(slice), finish(builder)))
        .record_batch();
  auto result = table_slice{std::move(batch), slice.schema()};
  result.import_time(slice.import_time());
  return result;
}

auto rows_are_contiguous(std::span<const int64_t> rows) -> bool {
  for (auto i = 1uz; i < rows.size(); ++i) {
    if (rows[i] != rows[i - 1] + 1) {
      return false;
    }
  }
  return true;
}

auto emit_group(std::vector<table_slice>& result, table_slice group_slice,
                null_pattern const& pattern,
                std::vector<ast::field_path> const& fields,
                diagnostic_handler& dh) -> void {
  auto fields_to_drop = fields_to_drop_for_pattern(pattern, fields);
  if (fields_to_drop.empty()) {
    result.push_back(std::move(group_slice));
  } else {
    result.push_back(tenzir::drop(group_slice, fields_to_drop, dh, false));
  }
}

auto drop_null_fields_ordered(table_slice slice,
                              const std::vector<ast::field_path>& fields,
                              std::span<const null_accessor> accessors,
                              diagnostic_handler& dh)
  -> std::vector<table_slice> {
  // Preserve row order by only merging adjacent rows with the same null shape.
  // Alternating shapes still fragment into many slices, but this path remains
  // valid for ordered pipelines and for slices whose event IDs encode position.
  auto words_per_pattern = (fields.size() + 63) / 64;
  auto previous_pattern = null_pattern(words_per_pattern, uint64_t{0});
  auto current_pattern = null_pattern(words_per_pattern, uint64_t{0});
  compute_null_pattern(accessors, 0, previous_pattern);
  auto result = std::vector<table_slice>{};
  auto emit_run = [&](size_t begin, size_t end, null_pattern const& pattern) {
    emit_group(result, subslice(slice, begin, end), pattern, fields, dh);
  };
  auto run_begin = 0uz;
  for (auto row = 1uz; row < slice.rows(); ++row) {
    compute_null_pattern(accessors, row, current_pattern);
    if (current_pattern == previous_pattern) {
      continue;
    }
    emit_run(run_begin, row, previous_pattern);
    run_begin = row;
    previous_pattern = current_pattern;
  }
  emit_run(run_begin, slice.rows(), previous_pattern);
  return result;
}

auto drop_null_fields_unordered(table_slice slice,
                                const std::vector<ast::field_path>& fields,
                                std::span<const null_accessor> accessors,
                                diagnostic_handler& dh)
  -> std::vector<table_slice> {
  // Group equal null shapes across the whole slice when ordering no longer
  // matters. This collapses alternating or high-cardinality input patterns
  // into fewer output slices, which avoids amplifying downstream per-slice work.
  auto words_per_pattern = (fields.size() + 63) / 64;
  auto current_pattern = null_pattern(words_per_pattern, uint64_t{0});
  auto bucket_index
    = std::unordered_map<null_pattern, size_t, null_pattern_hash>{};
  auto buckets = std::vector<bucket>{};
  bucket_index.reserve(slice.rows());
  for (auto row = 0uz; row < slice.rows(); ++row) {
    compute_null_pattern(accessors, row, current_pattern);
    auto [it, inserted]
      = bucket_index.try_emplace(current_pattern, buckets.size());
    if (inserted) {
      buckets.push_back({.pattern = current_pattern, .rows = {}});
    }
    buckets[it->second].rows.push_back(detail::narrow<int64_t>(row));
  }
  auto result = std::vector<table_slice>{};
  result.reserve(buckets.size());
  for (auto const& bucket : buckets) {
    auto const& rows = bucket.rows;
    if (rows.size() == slice.rows()) {
      emit_group(result, std::move(slice), bucket.pattern, fields, dh);
      continue;
    }
    auto group_slice = rows_are_contiguous(rows)
                         ? subslice(slice, rows.front(), rows.back() + 1)
                         : take_rows(slice, rows);
    emit_group(result, std::move(group_slice), bucket.pattern, fields, dh);
  }
  return result;
}

auto drop_null_fields_impl(table_slice slice,
                           const std::vector<ast::field_path>& selectors,
                           event_order order, diagnostic_handler& dh)
  -> std::vector<table_slice> {
  if (slice.rows() == 0) {
    return {table_slice{}};
  }
  auto fields = fields_to_check(slice, selectors);
  if (fields.empty()) {
    return {std::move(slice)};
  }
  auto field_offsets = resolve_field_paths(fields, slice.schema());
  auto accessors = build_null_accessors(slice, field_offsets);
  // The bucketed fast path may reorder non-contiguous rows. That is only safe
  // for explicitly unordered pipelines and for slices without offset metadata,
  // because otherwise regrouping rows would invent a dense event-ID range.
  if (order == event_order::unordered and slice.offset() == invalid_id) {
    return drop_null_fields_unordered(std::move(slice), fields, accessors, dh);
  }
  return drop_null_fields_ordered(std::move(slice), fields, accessors, dh);
}

struct DropNullFieldsArgs {
  std::vector<ast::expression> fields;
  event_order order = event_order::ordered;
};

class DropNullFields final : public Operator<table_slice, table_slice> {
public:
  explicit DropNullFields(DropNullFieldsArgs args) : order_{args.order} {
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
    auto output
      = drop_null_fields_impl(std::move(input), selectors_, order_, ctx.dh());
    for (auto& slice : output) {
      co_await push(std::move(slice));
    }
  }

private:
  std::vector<ast::field_path> selectors_;
  event_order order_ = event_order::ordered;
};

class drop_null_fields_operator final
  : public crtp_operator<drop_null_fields_operator> {
public:
  drop_null_fields_operator() = default;

  explicit drop_null_fields_operator(std::vector<ast::field_path> selectors,
                                     event_order order = event_order::ordered)
    : selectors_{std::move(selectors)}, order_{order} {
  }

  auto name() const -> std::string override {
    return "tql2.drop_null_fields";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      auto output = drop_null_fields_impl(std::move(slice), selectors_, order_,
                                          ctrl.diagnostics());
      for (auto& part : output) {
        co_yield std::move(part);
      }
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    return optimize_result{
      std::nullopt, order,
      std::make_unique<drop_null_fields_operator>(selectors_, order)};
  }

  friend auto inspect(auto& f, drop_null_fields_operator& x) -> bool {
    return f.object(x)
      .pretty_name("drop_null_fields_operator")
      .fields(f.field("selectors", x.selectors_), f.field("order", x.order_));
  }

private:
  std::vector<ast::field_path> selectors_;
  event_order order_ = event_order::ordered;
};

} // namespace

class plugin final : public virtual operator_plugin2<drop_null_fields_operator>,
                     public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<DropNullFieldsArgs, DropNullFields>{};
    auto fields
      = d.optional_variadic("fields", &DropNullFieldsArgs::fields, "field");
    d.optimization_order(&DropNullFieldsArgs::order);
    d.validate([=](DescribeCtx& ctx) -> Empty {
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
    return d.invariant_order();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_("drop_null_fields");
    auto selectors = std::vector<ast::field_path>{};
    // Special case: allow "drop_null_fields this" to behave like no arguments
    if (inv.args.size() == 1) {
      auto selector = ast::field_path::try_from(inv.args[0]);
      if (selector and selector->has_this() and selector->path().empty()) {
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
