//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/printable/tenzir/json.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/debug_writer.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/exec/operator.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/finalize_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/null_bitmap.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plan/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql/basic.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/try.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api.h>
#include <arrow/type.h>
#include <arrow/util/bitmap_writer.h>
#include <caf/actor_from_state.hpp>
#include <caf/expected.hpp>

namespace tenzir::plugins::where {

namespace {

// Selects matching rows from the input.
class where_operator final
  : public schematic_operator<where_operator, std::optional<expression>> {
public:
  where_operator() = default;

  /// Constructs a *where* pipeline operator.
  /// @pre *expr* must be normalized and validated
  explicit where_operator(located<expression> expr) : expr_{std::move(expr)} {
#if TENZIR_ENABLE_ASSERTIONS
    auto result = normalize_and_validate(expr_.inner);
    TENZIR_ASSERT(result, fmt::to_string(result.error()).c_str());
    TENZIR_ASSERT(*result == expr_.inner, fmt::to_string(result).c_str());
#endif // TENZIR_ENABLE_ASSERTIONS
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    auto ts = taxonomies{.concepts = modules::concepts()};
    auto resolved_expr = resolve(ts, expr_.inner, schema);
    if (not resolved_expr) {
      diagnostic::warning(resolved_expr.error())
        .primary(expr_.source)
        .emit(ctrl.diagnostics());
      return std::nullopt;
    }
    auto tailored_expr = tailor(std::move(*resolved_expr), schema);
    // We ideally want to warn when extractors can not be resolved. However,
    // this is tricky for e.g. `where #schema == "foo" && bar == 42` and
    // changing the behavior for this is tricky with the current expressions.
    if (not tailored_expr) {
      // diagnostic::warning(tailored_expr.error())
      //   .primary(expr_.source)
      //   .emit(ctrl.diagnostics());
      return std::nullopt;
    }
    return std::move(*tailored_expr);
  }

  auto process(table_slice slice, state_type& expr) const
    -> output_type override {
    // TODO: Adjust filter function return type.
    // TODO: Replace this with an Arrow-native filter function as soon as we
    // are able to directly evaluate expressions on a record batch.
    if (expr) {
      return filter(slice, *expr).value_or(table_slice{});
    }
    return {};
  }

  auto name() const -> std::string override {
    return "where";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    if (filter == trivially_true_expression()) {
      return optimize_result{expr_.inner, order, nullptr};
    }
    auto combined = normalize_and_validate(conjunction{expr_.inner, filter});
    TENZIR_ASSERT(combined);
    return optimize_result{std::move(*combined), order, nullptr};
  }

  friend auto inspect(auto& f, where_operator& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->fmt_value("({} @ {:?})", x.expr_.inner, x.expr_.source);
    }
    return f.apply(x.expr_);
  }

private:
  located<expression> expr_;
};

class tql1_plugin final : public virtual operator_plugin<where_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"where", "https://docs.tenzir.com/"
                                           "operators/where"};
    auto expr = located<tenzir::expression>{};
    parser.add(expr, "<expr>");
    parser.parse(p);
    auto normalized_and_validated = normalize_and_validate(expr.inner);
    if (! normalized_and_validated) {
      diagnostic::error("invalid expression")
        .primary(expr.source)
        .docs("https://tenzir.com/language/expressions")
        .throw_();
    }
    expr.inner = std::move(*normalized_and_validated);
    return std::make_unique<where_operator>(std::move(expr));
  }
};

auto filter2(const table_slice& slice, const ast::expression& expr,
             diagnostic_handler& dh, bool warn) -> std::vector<table_slice> {
  auto results = std::vector<table_slice>{};
  auto offset = int64_t{0};
  for (auto& filter : eval(expr, slice, dh)) {
    auto array = try_as<arrow::BooleanArray>(&*filter.array);
    if (not array) {
      diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
        .primary(expr)
        .emit(dh);
      offset += filter.array->length();
      continue;
    }
    if (array->true_count() == array->length()) {
      results.push_back(subslice(slice, offset, offset + array->length()));
      offset += array->length();
      continue;
    }
    if (warn) {
      diagnostic::warning("assertion failure").primary(expr).emit(dh);
    }
    auto length = array->length();
    auto current_value = array->Value(0);
    auto current_begin = int64_t{0};
    // We add an artificial `false` at index `length` to flush.
    for (auto i = int64_t{1}; i < length + 1; ++i) {
      const auto next = i != length && array->IsValid(i) && array->Value(i);
      if (current_value == next) {
        continue;
      }
      if (current_value) {
        results.push_back(subslice(slice, offset + current_begin, offset + i));
      }
      current_value = next;
      current_begin = i;
    }
    offset += length;
  }
  return results;
}

class where_assert_operator final
  : public crtp_operator<where_assert_operator> {
public:
  where_assert_operator() = default;

  where_assert_operator(ast::expression expr,
                        std::optional<ast::expression> msg, bool warn)
    : expr_{std::move(expr)}, msg_{std::move(msg)}, warn_{warn} {
  }

  auto name() const -> std::string override {
    return "where_assert_operator";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: This might be quite inefficient compared to what we could do.
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto offset = int64_t{0};
      for (auto& filter : eval(expr_, slice, ctrl.diagnostics())) {
        auto* array = try_as<arrow::BooleanArray>(*filter.array);
        if (not array) {
          diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
            .primary(expr_)
            .emit(ctrl.diagnostics());
          offset += filter.array->length();
          co_yield {};
          continue;
        }
        if (array->true_count() == array->length()) {
          co_yield subslice(slice, offset, offset + array->length());
          offset += array->length();
          continue;
        }
        if (array->null_count() > 0) {
          diagnostic::warning("expected `bool`, got `null`")
            .primary(expr_)
            .emit(ctrl.diagnostics());
        }
        if (warn_ and not msg_) {
          diagnostic::warning("assertion failure")
            .primary(expr_)
            .emit(ctrl.diagnostics());
        }
        auto length = array->length();
        auto current_value = array->Value(0);
        auto current_begin = int64_t{0};
        // We add an artificial `false` at index `length` to flush.
        auto results = std::vector<table_slice>{};
        const auto p = json_printer{json_printer_options{
          .tql = true,
          .oneline = true,
        }};
        auto buf = std::string{};
        const auto print_messages
          = [&](const int64_t start, const int64_t end) {
              if (start == end) {
                return;
              }
              const auto sub = subslice(slice, start, end);
              const auto ms = eval(*msg_, sub, ctrl.diagnostics());
              for (const auto& s : ms) {
                for (auto msg : s.values()) {
                  auto it = std::back_inserter(buf);
                  p.print(it, msg);
                  diagnostic::warning("assertion failed: {}", buf)
                    .primary(expr_)
                    .emit(ctrl.diagnostics());
                  buf.clear();
                }
              }
            };
        for (auto i = int64_t{1}; i < length + 1; ++i) {
          const auto next = i != length && array->IsValid(i) && array->Value(i);
          if (current_value == next) {
            continue;
          }
          if (current_value) {
            results.push_back(
              subslice(slice, offset + current_begin, offset + i));
          } else if (msg_) {
            print_messages(offset + current_begin, offset + i);
          }
          current_value = next;
          current_begin = i;
        }
        if (msg_) {
          print_messages(offset + current_begin, length);
        }
        co_yield concatenate(std::move(results));
        offset += length;
      }
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    if (warn_) {
      return optimize_result::order_invariant(*this, order);
    }
    auto [legacy, remainder] = split_legacy_expression(expr_);
    auto remainder_op = is_true_literal(remainder)
                          ? nullptr
                          : std::make_unique<where_assert_operator>(
                              std::move(remainder), msg_, warn_);
    if (filter == trivially_true_expression()) {
      return optimize_result{std::move(legacy), order, std::move(remainder_op)};
    }
    auto combined
      = normalize_and_validate(conjunction{std::move(legacy), filter});
    TENZIR_ASSERT(combined);
    return optimize_result{std::move(*combined), order,
                           std::move(remainder_op)};
  }

  friend auto inspect(auto& f, where_assert_operator& x) -> bool {
    return f.object(x).fields(f.field("expr_", x.expr_),
                              f.field("msg_", x.msg_),
                              f.field("warn_", x.warn_));
  }

private:
  ast::expression expr_;
  std::optional<ast::expression> msg_;
  bool warn_{};
};

struct arguments {
  ast::expression field;
  ast::lambda_expr lambda;

  static auto parse(const std::string& name, const std::string& lambda_name,
                    const std::string& lambda_hint,
                    const function_plugin::invocation& inv, session ctx)
    -> failure_or<arguments> {
    auto dh = collecting_diagnostic_handler{};
    auto sp = session_provider::make(dh);
    auto args = arguments{};
    if (argument_parser2::function(name)
          .positional("list", args.field, "list")
          .positional(lambda_name, args.lambda, lambda_hint)
          .parse(inv, sp.as_session())) {
      std::move(dh).forward_to(ctx);
      return args;
    }
    auto diags = std::exchange(dh, {}).collect();
    auto expr = ast::expression{};
    if (argument_parser2::function(name)
          .positional("list", args.field, "list")
          .positional("x", expr, "any")
          .positional("expr", args.lambda.right, "any")
          .parse(inv, sp.as_session())) {
      diagnostic::warning("deprecated; please use a lambda expression instead")
        .primary(expr.get_location().combine(args.lambda.right))
        .hint("instead of `x, y`, provide `x => y`")
        .emit(ctx);
      std::move(dh).forward_to(ctx);
      auto* field = try_as<ast::root_field>(expr);
      if (not field or field->has_question_mark) {
        diagnostic::error("expected identifier").primary(expr).emit(ctx);
        return failure::promise();
      }
      args.lambda.left = std::move(field->id);
      return args;
    }
    for (auto& diag : diags) {
      ctx.dh().emit(std::move(diag));
    }
    return args;
  }
};

auto make_where_function(function_plugin::invocation inv, session ctx)
  -> failure_or<function_ptr> {
  TRY(auto args,
      arguments::parse("where", "predicate", "any => bool", inv, ctx));
  return function_use::make(
    [args = std::move(args)](function_plugin::evaluator eval, session ctx) {
      return map_series(eval(args.field), [&](series field) -> multi_series {
        return match(
          field.type,
          [&](const null_type&) -> series {
            return field;
          },
          [&](const list_type&) -> series {
            const auto lists = check(field.as<list_type>());
            const auto list_values
              = series{lists.type.value_type(), lists.array->values()};
            auto ids = null_bitmap{};
            auto all_true = true;
            auto all_false = true;
            // TODO: Technically, this call to `evaluate` can cause warnings, as
            // lists may contain bogus values in the value array where the list
            // itself is `null`. This is very unlikely to happen in practice,
            // and one proper fix for this would be passing in a null bitmap to
            // the call to evaluate to indicate which rows not to evaluate.
            for (const auto& result : eval(args.lambda, lists)) {
              match(
                result.type,
                [&](const bool_type&) {
                  const auto pred = check(result.as<bool_type>());
                  if (pred.array->true_count() == pred.length()) {
                    all_false = false;
                    ids.append_bits(true, pred.length());
                    return;
                  }
                  all_true = false;
                  if (pred.array->null_count() > 0) {
                    diagnostic::warning("expected `bool`, got `null`")
                      .primary(args.lambda.right)
                      .emit(ctx);
                  }
                  if (pred.array->true_count() == 0) {
                    ids.append_bits(false, pred.length());
                    return;
                  }
                  all_false = false;
                  for (const auto& elem : *pred.array) {
                    ids.append_bit(elem.value_or(false));
                  }
                },
                [&](const auto&) {
                  diagnostic::warning("expected `bool`, got `{}`",
                                      result.type.kind())
                    .primary(args.lambda.right)
                    .emit(ctx);
                  all_true = false;
                  ids.append_bits(false, result.length());
                });
            }
            TENZIR_ASSERT(list_values.length()
                          == detail::narrow<int64_t>(ids.size()));
            if (all_true) {
              return field;
            }
            if (all_false) {
              auto builder = series_builder{field.type};
              for (int64_t i = 0; i < lists.length(); ++i) {
                builder.list();
              }
              return builder.finish_assert_one_array();
            }
            auto builder = series_builder{field.type};
            match(list_values.type, [&](const auto& list_values_type) {
              for (int64_t i = 0; i < lists.length(); ++i) {
                if (lists.array->IsNull(i)) {
                  builder.null();
                  continue;
                }
                auto list_builder = builder.list();
                const auto offset = lists.array->value_offset(i);
                const auto length = lists.array->value_length(i);
                for (auto j = offset; j < offset + length; ++j) {
                  if (not ids[j]) {
                    continue;
                  }
                  if (list_values.array->IsNull(j)) {
                    list_builder.null();
                    continue;
                  }
                  list_builder.data(
                    value_at(list_values_type, *list_values.array, j));
                }
              }
            });
            return builder.finish_assert_one_array();
          },
          [&](const auto&) -> series {
            diagnostic::warning("expected `list`, got `{}`", field.type.kind())
              .primary(args.field)
              .emit(ctx);
            return series::null(null_type{}, field.length());
          });
      });
    });
}

struct where_result_part {
  struct part_slice_info {
    size_t part{};
    size_t slice_start{};
    size_t slice_end{};

    auto size() const -> size_t {
      return slice_end - slice_start;
    }
  };
  std::vector<part_slice_info> slices;
  arrow::Int32Builder offset_builder;
  arrow::TypedBufferBuilder<bool> null_builder;
  int64_t null_count = 0;
  int64_t event_count = 0;

  auto physical_size() const -> size_t {
    auto sum = size_t{0};
    for (const auto& slice : slices) {
      sum += slice.size();
    }
    return sum;
  }

  auto add_null() -> void {
    ++event_count;
    ++null_count;
    check(null_builder.Append(false));
    if (offset_builder.length() == 0) {
      check(offset_builder.Append(0));
    }
    check(offset_builder.Append(
      offset_builder.GetValue(offset_builder.length() - 1)));
  }

  auto add_empty() -> void {
    ++event_count;
    check(null_builder.Append(true));
    if (offset_builder.length() == 0) {
      check(offset_builder.Append(0));
    }
    check(offset_builder.Append(
      offset_builder.GetValue(offset_builder.length() - 1)));
  }

  auto add_list(size_t current_part_index, int64_t n) -> void {
    ++event_count;
    check(null_builder.Append(true));
    if (offset_builder.length() == 0) {
      check(offset_builder.Append(0));
    }
    check(offset_builder.Append(
      offset_builder.GetValue(offset_builder.length() - 1) + n));
    if (slices.empty() or current_part_index != slices.back().part) {
      slices.emplace_back(current_part_index, 0, n);
      return;
    }
    slices.back().slice_end += n;
  }
};

auto make_map_function(function_plugin::invocation inv, session ctx)
  -> failure_or<function_ptr> {
  TRY(auto args, arguments::parse("map", "function", "any -> any", inv, ctx));
  return function_use::make([args = std::move(args)](
                              function_plugin::evaluator eval, session ctx) {
    return map_series(eval(args.field), [&](series field) -> multi_series {
      if (field.as<null_type>()) {
        return field;
      }
      auto field_list = field.as<list_type>();
      if (not field_list) {
        diagnostic::warning("expected `list`, but got `{}`", field.type.kind())
          .primary(args.field)
          .emit(ctx);
        return series::null(null_type{}, eval.length());
      }
      auto list_values
        = series{field_list->type.value_type(), field_list->array->values()};
      if (list_values.length() == 0) {
        return make_list_series(
          series::null(null_type{}, field_list->array->values()->length()),
          *field_list->array);
      }
      auto ms = eval(args.lambda, *field_list);
      TENZIR_ASSERT(not ms.parts().empty());
      // If there were no conflicts in the result, we are in the happy case
      // Here we just need to take that slice and re-join it with the offsets
      // from the input.
      const auto n_parts = ms.parts().size();
      if (n_parts == 1) {
        auto& values = ms.parts().front();
        return series{
          list_type{values.type},
          std::make_shared<arrow::ListArray>(
            list_type{values.type}.to_arrow_type(), field_list->array->length(),
            field_list->array->value_offsets(), values.array,
            field_list->array->null_bitmap(), field_list->array->null_count(),
            field_list->array->offset()),
        };
      }
      // If there is more than one part, we need to rebuild batches by merging
      // the parts that should be part of the same event/list and splitting
      // others.
      //
      // The strategy is:
      // * Iterate all events of the input
      //    * collect the largest possible slices of `ms` and the offsets we
      //      would have in those slices
      //    * Conflicts within a list are detected by keeping track of a
      //      `running_ms_offset` and comparing that against the current
      //      `field_offset`.
      //    * If there is a conflict, we collect slices of multiple parts
      //    * Those slices get merged in the end
      // Every part of the result is made up of one or more slices that need
      // to be merged and an offset builder
      // TODO:
      // * In the spirit of creating the largest possible batches, the
      //   implementation causes null lists and empty lists to be changed to
      //   the type of the next/previous non-empty list. That is not entirely
      //   correct, but seems like an acceptable tradeoff.
      // * we could do the slicing early instead of storing indices to slice by
      // * strictly speaking the entire `result_assembly_info` isn't necessary,
      //   but it greatly reduces confusion.
      auto result_assembly_info = std::vector<where_result_part>{};
      result_assembly_info.reserve(3);
      // Putting this starting info in early, allows us to safely use `back()`
      result_assembly_info.emplace_back();
      auto current_part_index = size_t{0};
      auto current_part_offset = int64_t{0};
      auto running_ms_offset = ms.part(0).length();
      const auto current_part_length = [&]() {
        return ms.part(current_part_index).length();
      };
      const auto advance_current_input_part = [&]() {
        ++current_part_index;
        current_part_offset = 0;
        if (current_part_index < n_parts) {
          running_ms_offset += current_part_length();
        }
      };
      struct list_info_t {
        int64_t event_start_offset;
        int64_t event_list_size;
        int64_t event_end_offset;
      };
      const auto n_events = field_list->length();
      auto consume_remainder = false;
      for (int64_t event_index = 0; event_index < n_events; ++event_index) {
        // A list that is null is not part of the offsets and has no length.
        // We just collect a null in this case, which will be included in the
        // current result part. Its arguable if this differently typed null is
        // correct, but it makes for larger parts.
        if (field_list->array->IsNull(event_index)) {
          result_assembly_info.back().add_null();
          continue;
        }
        const auto event_start_offset
          = field_list->array->value_offset(event_index);
        const auto event_list_size
          = static_cast<int64_t>(field_list->array->value_length(event_index));
        const auto event_end_offset = event_start_offset + event_list_size;
        // If the current event is fully covered by the current part
        if (event_list_size == 0) {
          result_assembly_info.back().add_empty();
          continue;
        }
        if (event_end_offset <= running_ms_offset) {
          current_part_offset += event_list_size;
          result_assembly_info.back().add_list(current_part_index,
                                               event_list_size);
          // If the end of the list perfectly aligns with a ms.part boundary
          if (consume_remainder) {
            continue;
          }
          if (event_end_offset == running_ms_offset) {
            // If its the last event, we dont append anything
            if (event_index == n_events - 1) {
              break;
            }
            // This is special handling to consume trailing null/empty lists
            // from the input. It will continue the `event_index` loop appending
            // nulls/empty lists, but not creating any new parts. This
            // effectively merges all trailing null/empty lists into the last
            // part.
            if (running_ms_offset == ms.length()) {
              consume_remainder = true;
              continue;
            }
            // We advance the current part and create a new result part.
            advance_current_input_part();
            // We create a new result part
            result_assembly_info.emplace_back();
          }
          continue;
        }
        // We need to create a new assembly for the merge.
        if (result_assembly_info.back().physical_size() > 0) {
          result_assembly_info.emplace_back();
        } else {
          // It is possible that we were already building a non-physical (all
          // null/empty) part here. In that case, we need to clear the current
          // empty slice.
          if (result_assembly_info.back().slices.size() != 0) {
            TENZIR_ASSERT(result_assembly_info.back().slices.size() == 1);
            TENZIR_ASSERT(result_assembly_info.back().slices.front().size()
                          == 0);
            result_assembly_info.back().slices.clear();
          }
        }
        // Build up the merging part.
        auto& merging_part = result_assembly_info.back();
        merging_part.event_count += 1;
        // Because we only resolve conflicts with a single element, we know that
        // there is only two offsets for this: 0 and the sizeof the list.
        // We write the first (0) offset only conditionally, because there may
        // already be an empty list stored in it.
        if (merging_part.offset_builder.length() == 0) {
          check(merging_part.offset_builder.Append(0));
        }
        check(merging_part.offset_builder.Append(event_list_size));
        // Additionally, we know that it will not be null, because otherwise we
        // wouldn't have reached this point.
        check(merging_part.null_builder.Append(true));
        // Now we collect parts of ms until we have enough elements for this event
        auto remaining_length = event_list_size;
        do {
          const auto take_from_current = std::min(
            remaining_length, current_part_length() - current_part_offset);
          remaining_length -= take_from_current;
          TENZIR_ASSERT(take_from_current > 0);
          merging_part.slices.emplace_back(
            current_part_index, current_part_offset,
            current_part_offset + take_from_current);
          current_part_offset += take_from_current;
          TENZIR_ASSERT(current_part_offset <= current_part_length());
          if (current_part_offset == current_part_length()) {
            advance_current_input_part();
          }
        } while (remaining_length != 0);
        TENZIR_ASSERT(remaining_length == 0);
      }
      TENZIR_ASSERT(running_ms_offset == ms.length());
      // Finally, we assemble the result from the info we built
      auto result = std::vector<series>{};
      result.reserve(result_assembly_info.size());
      auto to_merge = multi_series{};
      for (auto& p : result_assembly_info) {
        TENZIR_ASSERT(p.null_builder.length() == p.event_count);
        TENZIR_ASSERT(p.null_builder.length() > 0);
        TENZIR_ASSERT(p.offset_builder.length() != 1);
        to_merge.clear();
        for (auto& [idx, start, end] : p.slices) {
          to_merge.append(ms.part(idx).slice(start, end));
        }
        auto [merged_series, merge_status, conflicts] = to_merge.to_series(
          multi_series::to_series_strategy::take_largest_from_start_null_rest);
        TENZIR_ASSERT(merge_status
                      != multi_series::to_series_result::status::fail);
        auto offsets = check(p.offset_builder.Finish());
        auto validity = check(p.null_builder.FinishWithLength(p.event_count));
        result.emplace_back(list_type{merged_series.type},
                            check(arrow::ListArray::FromArrays(
                              *offsets, *merged_series.array,
                              arrow::default_memory_pool(), std::move(validity),
                              p.null_count)));
        if (merge_status != multi_series::to_series_result::status::ok) {
          /// This produces prettier error messages for the common case
          auto kinds = std::set<type_kind>{};
          for (const auto& c : conflicts) {
            kinds.insert(c.kind());
          }
          auto primary = std::string{};
          auto note = std::string{};
          if (kinds.size() == 1) {
            primary = fmt::format("`{}` are incompatible",
                                  fmt::join(conflicts, "`, `"));
            note = fmt::format("all entries that are not compatible with `{}` "
                               "will be "
                               "`null`",
                               merged_series.type);
          } else {
            primary
              = fmt::format("`{}` are incompatible", fmt::join(kinds, "`, `"));
            note = fmt::format("all entries that are not compatible with `{}` "
                               "will be "
                               "`null`",
                               merged_series.type.kind());
          }
          diagnostic::warning(
            "lambda must evaluate to compatible types within the same list")
            .primary(args.lambda.right, primary)
            .note(note)
            .emit(ctx);
        }
      }
      return multi_series{std::move(result)};
    });
  });
}

using where_assert_plugin = operator_inspection_plugin<where_assert_operator>;

class assert_plugin final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.assert";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto expr = ast::expression{};
    auto msg = std::optional<ast::expression>{};
    TRY(argument_parser2::operator_("assert")
          .positional("invariant", expr, "bool")
          .named("message", msg, "string")
          .parse(inv, ctx));
    return std::make_unique<where_assert_operator>(std::move(expr),
                                                   std::move(msg), true);
  }
};

#if 0
class where_exec : public exec::operator_base<ast::expression> {
public:
  explicit where_exec(initializer init) : operator_base{std::move(init)} {
  }

  void next(const table_slice& slice) override {
    const auto& pred = state();
    const auto filters = eval(pred, slice, ctx());
    auto filter_offset = int64_t{0};
    for (const auto& part : filters.parts()) {
      const auto filter = part.as<bool_type>();
      if (not filter) {
        diagnostic::warning("expected `bool`, got `{}`", part.type.kind())
          .primary(pred)
          .emit(ctx());
        filter_offset += part.length();
        continue;
      }
      if (filter->array->true_count() == filter->length()) {
        push(subslice(slice, filter_offset, filter_offset + filter->length()));
        filter_offset += filter->length();
        continue;
      }
      // TODO: Should this warn? For some reason the original TQL2
      // implementation did not.
      // if (filter->array->null_count() > 0) {
      //   diagnostic::warning(…).emit(ctx());
      // }
      auto start = std::optional<int64_t>{};
      for (int64_t i = 0; i < filter->length(); ++i) {
        if (filter->array->IsValid(i) and filter->array->GetView(i)) {
          if (not start) {
            start.emplace(i);
          }
          continue;
        }
        if (start) {
          push(subslice(slice, filter_offset + *start, filter_offset + i));
          start.reset();
        }
      }
      if (start) {
        push(subslice(slice, filter_offset + *start,
                      filter_offset + filter->length()));
      }
      filter_offset += filter->length();
    }
    ready();
  }

  auto should_stop() -> bool override {
    return get_input_ended();
  }
};
#endif

class Where final : public Operator<table_slice, table_slice> {
public:
  explicit Where(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto process(table_slice input, Push<table_slice>& push, AsyncCtx& ctx)
    -> Task<void> override {
    for (auto output : filter2(input, expr_, ctx, false)) {
      co_await push(std::move(output));
    }
  }

  friend auto inspect(auto& f, Where& x) -> bool {
    return f.apply(x.expr_);
  }

private:
  ast::expression expr_;
};

// TODO: Don't want to write this fully ourselves.
class where_plan final : public plan::operator_base {
public:
  where_plan() = default;

  auto name() const -> std::string override {
    return "where_plan";
  }

  explicit where_plan(ast::expression predicate)
    : predicate_{std::move(predicate)} {
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    TENZIR_TODO();
    // return exec::spawn_operator<where_exec>(std::move(args), predicate_);
  }

  auto spawn(std::optional<chunk_ptr> restore) && -> OperatorPtr override {
    return std::make_unique<Where>(predicate_);
  }

  friend auto inspect(auto& f, where_plan& x) -> bool {
    return f.apply(x.predicate_);
  }

private:
  ast::expression predicate_;
};

// TODO: Don't want to write this fully ourselves.
class where_ir final : public ir::operator_base {
public:
  where_ir() = default;

  where_ir(location self, ast::expression predicate)
    : self_{self}, predicate_{std::move(predicate)} {
  }

  auto name() const -> std::string override {
    return "where_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    (void)instantiate;
    TRY(predicate_.substitute(ctx));
    return {};
  }

  // TODO: Should this get the type of the input?
  // Or do we get it earlier? Or later?
  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    (void)ctx;
    return std::make_unique<where_plan>(std::move(predicate_));
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      // TODO: Do not duplicate these messages across the codebase.
      diagnostic::error("operator expects events").primary(self_).emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // TODO: Shall we avoid optimizing if it doesn't make sense?
    filter.insert(filter.begin(), std::move(predicate_));
    return ir::optimize_result{std::move(filter), order, {}};
  }

  friend auto inspect(auto& f, where_ir& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("predicate", x.predicate_));
  }

private:
  location self_;
  ast::expression predicate_;
};

TENZIR_REGISTER_PLUGIN(inspection_plugin<ir::operator_base, where_ir>)
TENZIR_REGISTER_PLUGIN(inspection_plugin<plan::operator_base, where_plan>)

class where_plugin final : public virtual operator_factory_plugin,
                           public virtual function_plugin,
                           public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.where";
  }

  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::operator_("where")
          .positional("predicate", expr, "bool")
          .parse(inv, ctx));
    return std::make_unique<where_assert_operator>(std::move(expr),
                                                   std::nullopt, false);
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> override {
    auto expr = ast::expression{};
    // TODO: We don't want to create a session here. This is just a test to see
    // how far we could go with the existing argument parser.
    auto provider = session_provider::make(ctx);
    auto loc = inv.op.get_location();
    TRY(argument_parser2::operator_("where")
          .positional("predicate", expr, "bool")
          .parse(operator_factory_plugin::invocation{std::move(inv.op),
                                                     std::move(inv.args)},
                 provider.as_session()));
    TRY(expr.bind(ctx));
    return std::make_unique<where_ir>(loc, std::move(expr));
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_plugin::invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return make_where_function(std::move(inv), ctx);
  }
};

class map_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.map";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return make_map_function(std::move(inv), ctx);
  }
};

} // namespace
} // namespace tenzir::plugins::where

TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::tql1_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::assert_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::where_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::where_assert_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::map_plugin)
