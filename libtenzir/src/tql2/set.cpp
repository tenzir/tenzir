//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/set.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api_scalar.h>
#include <caf/detail/is_complete.hpp>
#include <caf/detail/is_one_of.hpp>

#include <type_traits>

namespace tenzir {

namespace {

auto rebatch_events(std::vector<table_slice> events)
  -> std::vector<table_slice> {
  if (events.size() < 2) {
    return events;
  }
  auto result = std::vector<table_slice>{};
  auto start = events.begin();
  auto rows = start->rows();
  const auto end = events.end();
  for (auto it = std::next(start); it < end; ++it) {
    rows += it->rows();
    if (it->schema() == start->schema()
        and rows < defaults::import::table_slice_size) {
      continue;
    }
    result.push_back(concatenate({start, it}));
    start = it;
    rows = start->rows();
  }
  result.push_back(concatenate({start, end}));
  return result;
}

} // namespace

auto consume_path(std::span<const ast::field_path::segment> path, series value)
  -> series {
  if (path.empty()) {
    return value;
  }
  value = consume_path(path.subspan(1), std::move(value));
  auto new_type
    = type{record_type{record_type::field_view{path[0].id.name, value.type}}};
  auto new_array = make_struct_array(value.length(), nullptr, {path[0].id.name},
                                     {value.array}, as<record_type>(new_type));
  return series{
    std::move(new_type),
    std::move(new_array),
  };
}

auto assign(std::span<const ast::field_path::segment> left, series right,
            series input, diagnostic_handler& dh, assign_position position)
  -> series {
  TENZIR_ASSERT(right.length() == input.length());
  if (left.empty()) {
    return right;
  }
  if (input.type.kind().is<null_type>()) {
    // We silently upgrade the null type to a record. We could also warn here,
    // but then we should perhaps adjust the code below to check for `null`
    // values assigning to a `record` type.
    return consume_path(left, std::move(right));
  }
  auto* rec_ty = try_as<record_type>(input.type);
  if (not rec_ty) {
    diagnostic::warning("implicit record for `{}` field overwrites `{}` value",
                        left[0].id.name, input.type.kind())
      .primary(left[0].id)
      .hint("if this is intentional, drop the parent field before")
      .emit(dh);
    return consume_path(left, std::move(right));
  }
  auto& array = as<arrow::StructArray>(*input.array);
  auto new_ty_fields = collect(rec_ty->fields());
  // We flatten the input fields here because the input `{foo: null}` of type
  // `{foo: {bar: int64}` should become `{foo: {bar: null, qux: 42}}` for the
  // assignment `foo.qux = 42`. This is consistent with the behavior for when
  // `foo` is of type `null` or does not exist. Also, simply using `.fields()`
  // here would not propagate the `null` values from the parent record.
  auto new_field_arrays = std::invoke([&] {
    if (array.null_count() == 0) {
      return array.fields();
    }
    return check(array.Flatten());
  });
  auto index = std::optional<size_t>{};
  for (auto [i, field] : detail::enumerate(new_ty_fields)) {
    if (field.name == left[0].id.name) {
      index = i;
      break;
    }
  }
  if (index) {
    auto& field_ty = new_ty_fields[*index].type;
    auto& field_array = new_field_arrays[*index];
    auto new_field = assign(left.subspan(1), std::move(right),
                            series{field_ty, field_array}, dh, position);
    field_ty = std::move(new_field.type);
    field_array = std::move(new_field.array);
  } else {
    auto inner = consume_path(left.subspan(1), std::move(right));
    switch (position) {
      case tenzir::assign_position::front: {
        new_ty_fields.emplace(new_ty_fields.begin(), left[0].id.name,
                              inner.type);
        new_field_arrays.emplace(new_field_arrays.begin(), inner.array);
        break;
      }
      case tenzir::assign_position::back: {
        new_ty_fields.emplace_back(left[0].id.name, inner.type);
        new_field_arrays.push_back(inner.array);
        break;
      }
    }
  }
  auto new_ty_field_names
    = new_ty_fields | std::views::transform(&record_type::field_view::name);
  auto new_type = type{record_type{new_ty_fields}};
  auto new_array
    = make_struct_array(array.length(), nullptr,
                        std::vector<std::string>{new_ty_field_names.begin(),
                                                 new_ty_field_names.end()},
                        new_field_arrays, as<record_type>(new_type));
  // TODO: What to do with metadata on record?
  // new_type.assign_metadata(input.type);
  return series{std::move(new_type), std::move(new_array)};
}

auto assign(const ast::meta& left, const series& right,
            const table_slice& input, diagnostic_handler& diag)
  -> std::vector<table_slice> {
  auto transform = [&input]<std::derived_from<arrow::Array> Array>(
                     const Array& array, auto&& f) {
    using ty = type_from_arrow_t<Array>;
    static_assert(basic_type<ty>);
    TENZIR_ASSERT(array.length() > 0);
    auto get_value = [&](int64_t i) {
      return array.IsValid(i) ? std::optional{value_at(ty{}, array, i)}
                              : std::nullopt;
    };
    auto result = std::vector<table_slice>{};
    auto begin = int64_t{0};
    auto begin_value = get_value(begin);
    for (auto i = int64_t{0}; i < array.length() + 1; ++i) {
      auto emit = i == array.length() || get_value(i) != begin_value;
      if (emit) {
        result.emplace_back(f(subslice(input, begin, i), begin_value));
        begin = i;
        if (i != array.length()) {
          begin_value = get_value(begin);
        }
      }
    }
    return result;
  };
  auto original = [&input] {
    auto result = std::vector<table_slice>{};
    result.push_back(input);
    return result;
  };
  // TODO: The locations used in the warnings below point to the value. Maybe
  // they should point to the metadata selector, or the equal token instead?
  switch (left.kind) {
    case ast::meta::name: {
      // TODO: We considered to make the schema name optional at some point.
      // This would help here, as we could set the name to `null` if we get a
      // non-string type or a null string. Instead, we keep the original schema
      // name in both cases for now.
      auto* array = dynamic_cast<arrow::StringArray*>(right.array.get());
      if (not array) {
        diagnostic::warning("expected string but got {}", right.type.kind())
          .primary(left)
          .emit(diag);
        return original();
      }
      return transform(*array, [&](table_slice slice,
                                   std::optional<std::string_view> value) {
        if (not value) {
          diagnostic::warning("schema name must not be `null`")
            .primary(left)
            .emit(diag);
          return slice;
        }
        auto new_type = type{
          *value,
          slice.schema(),
          collect(slice.schema().attributes()),
        };
        auto new_batch = check(
          to_record_batch(slice)->ReplaceSchema(new_type.to_arrow_schema()));
        return table_slice{new_batch, std::move(new_type)};
      });
    }
    case ast::meta::import_time: {
      // If the right side is not a timestamp, we do the same as if the value is
      // null. On an implementation level, the import time is not nullable, but
      // we use the default-constructed time as a marker for `null`. This is
      // translated back to `null` when it is read by the evaluator.
      auto* array = dynamic_cast<arrow::TimestampArray*>(right.array.get());
      if (not array) {
        if (not right.type.kind().is<null_type>()) {
          diagnostic::warning("expected `time` but got `{}`", right.type.kind())
            .primary(left)
            .emit(diag);
        }
        auto copy = input;
        copy.import_time(time{});
        return {std::move(copy)};
      }
      return transform(*array, [&](table_slice slice,
                                   std::optional<time> value) {
        if (not value) {
          value = time{};
        } else if (value == time{}) {
          // This means that we are trying to set a non-null
          // time which would be interpreted as null later.
          diagnostic::warning("import time cannot be `{}`", time{})
            .primary(left)
            .hint("consider using `null` instead")
            .emit(diag);
        }
        slice.import_time(*value);
        return slice;
      });
    }
    case ast::meta::internal: {
      // TODO: If this is set to null, we keep the original setting for now. We
      // could instead also set it to `false`, as `null` is also treated as
      // `false` in contexts such as `where` or `if`.
      auto* values = dynamic_cast<arrow::BooleanArray*>(right.array.get());
      if (not values) {
        diagnostic::warning("expected bool but got {}", right.type.kind())
          .primary(left)
          .emit(diag);
        return original();
      }
      return transform(*values, [&](table_slice slice,
                                    std::optional<bool> value) {
        if (not value) {
          diagnostic::warning("cannot set `@internal` to `null`")
            .primary(left)
            .emit(diag);
          return slice;
        }
        auto previous = slice.schema().attribute("internal").has_value();
        if (*value == previous) {
          return slice;
        }
        auto new_attributes = std::vector<type::attribute_view>{};
        for (auto [key, value] : input.schema().attributes()) {
          if (key == "internal") {
            continue;
          }
          new_attributes.emplace_back(key, value);
        }
        if (*value) {
          new_attributes.emplace_back("internal", "");
        }
        auto new_type = type{
          input.schema().name(),
          as<record_type>(input.schema()),
          std::move(new_attributes),
        };
        auto new_batch = check(
          to_record_batch(input)->ReplaceSchema(new_type.to_arrow_schema()));
        return table_slice{new_batch, std::move(new_type)};
      });
    }
  }
  TENZIR_UNREACHABLE();
}

auto assign(const ast::field_path& left, series right, const table_slice& input,
            diagnostic_handler& dh, assign_position position) -> table_slice {
  auto result
    = assign(left.path(), std::move(right), series{input}, dh, position);
  auto* rec_ty = try_as<record_type>(result.type);
  if (not rec_ty) {
    diagnostic::warning("assignment to `this` requires `record`, but got `{}`",
                        result.type.kind())
      .primary(left)
      .emit(dh);
    result = {record_type{},
              make_struct_array(result.length(), nullptr, {}, record_type{})};
  }
  result.type.assign_metadata(input.schema());
  auto schema = arrow::schema(result.array->type()->fields(),
                              to_record_batch(input)->schema()->metadata());
  auto slice = table_slice{
    arrow::RecordBatch::Make(std::move(schema), result.length(),
                             as<arrow::StructArray>(*result.array).fields()),
    std::move(result.type),
  };
  slice.import_time(input.import_time());
  return slice;
}

auto assign(const ast::selector& left, series right, const table_slice& input,
            diagnostic_handler& dh, assign_position position)
  -> std::vector<table_slice> {
  return left.match(
    [&](const ast::meta& left) {
      return assign(left, right, input, dh);
    },
    [&](const ast::field_path& left) {
      auto result = std::vector<table_slice>{};
      result.push_back(assign(left, std::move(right), input, dh, position));
      return result;
    });
}

auto resolve_move_keyword(ast::assignment assignment)
  -> std::pair<ast::assignment, std::vector<ast::field_path>> {
  auto out = std::vector<ast::field_path>{};
  auto recurse = [&](const auto& recurse, ast::expression& expr,
                     std::vector<ast::field_path>& out) -> void {
    match(
      expr,
      [&](ast::unary_expr& x) {
        recurse(recurse, x.expr, out);
        if (x.op.inner == ast::unary_op::move) {
          if (auto field = ast::field_path::try_from(x.expr)) {
            out.push_back(std::move(*field));
            expr = std::move(x.expr);
          }
        }
      },
      [&](ast::record& x) {
        for (auto& item : x.items) {
          match(
            item,
            [&](ast::spread& x) {
              recurse(recurse, x.expr, out);
            },
            [&](ast::record::field& x) {
              recurse(recurse, x.expr, out);
            });
        }
      },
      [&](ast::list& x) {
        for (auto& item : x.items) {
          match(
            item,
            [&](ast::spread& x) {
              recurse(recurse, x.expr, out);
            },
            [&](ast::expression& x) {
              recurse(recurse, x, out);
            });
        }
      },
      [](ast::constant&) {}, [](ast::pipeline_expr&) {},
      [](ast::root_field&) {}, [](ast::this_&) {}, [](ast::meta&) {},
      [](ast::dollar_var&) {},
      [&](ast::assignment& x) {
        recurse(recurse, x.right, out);
      },
      [&](ast::unpack& x) {
        recurse(recurse, x.expr, out);
      },
      [](ast::underscore&) {},
      [&](ast::function_call& x) {
        // TODO: The `map` and `where` functions abuse the expresions they
        // take as arguments as a poor-mans lambda expression. We don't recurse
        // further when we encounter them here, but that's at best a stopgap.
        // Ideally, there'd be proper lambda support in the language itself.
        if (x.fn.path.size() == 1) {
          const auto& name = x.fn.path.front().name;
          if (name == "map" or name == "where") {
            return;
          }
        }
        for (auto& arg : x.args) {
          recurse(recurse, arg, out);
        }
      },
      [&](ast::binary_expr& x) {
        recurse(recurse, x.left, out);
        recurse(recurse, x.right, out);
      },
      [&](ast::index_expr& x) {
        recurse(recurse, x.expr, out);
        recurse(recurse, x.index, out);
      },
      [&](ast::field_access& x) {
        recurse(recurse, x.left, out);
      });
  };
  recurse(recurse, assignment.right, out);
  return {std::move(assignment), std::move(out)};
}

auto drop(const table_slice& slice, const std::vector<ast::field_path>& fields,
          diagnostic_handler& dh) -> table_slice {
  constexpr auto drop = [](auto&&...) {
    return indexed_transformation::result_type{};
  };
  auto offsets = std::unordered_set<offset>{};
  for (const auto& field : fields) {
    match(
      resolve(field, slice.schema()),
      [&](const offset& of) {
        offsets.insert(of);
      },
      [&](const resolve_error& err) {
        match(
          err.reason,
          [&](const resolve_error::field_not_found&) {
            diagnostic::warning("field `{}` not found", err.ident.name)
              .primary(err.ident, "use `?` to suppress this warning")
              .emit(dh);
          },
          [&](const resolve_error::field_not_found_no_error&) {},
          [&](const resolve_error::field_of_non_record& reason) {
            diagnostic::warning("type `{}` has no field `{}`",
                                reason.type.kind(), err.ident.name)
              .primary(err.ident)
              .emit(dh);
          });
      });
  }
  auto ts = std::vector<indexed_transformation>{};
  for (const auto& of : offsets) {
    ts.emplace_back(of, drop);
  }
  return transform_columns(slice, ts);
}

auto set_operator::operator()(generator<table_slice> input,
                              operator_control_plane& ctrl) const
  -> generator<table_slice> {
  for (auto&& slice : input) {
    if (slice.rows() == 0) {
      co_yield {};
      continue;
    }
    if (assignments_.empty()) {
      co_yield std::move(slice);
      continue;
    }
    // The right-hand side is always evaluated with the original input, because
    // side-effects from preceding assignments shall not be reflected when
    // calculating the value of the left-hand side.
    auto values = std::vector<multi_series>{};
    for (const auto& assignment : assignments_) {
      values.push_back(eval(assignment.right, slice, ctrl.diagnostics()));
    }
    slice = drop(slice, moved_fields_, ctrl.diagnostics());
    // After we know all the multi series values on the right, we can split the
    // input table slice and perform the actual assignment.
    auto begin = int64_t{0};
    auto results = std::vector<table_slice>{};
    for (auto values_slice : split_multi_series(values)) {
      TENZIR_ASSERT(not values_slice.empty());
      auto end = begin + values_slice[0].length();
      // We could still perform further splits if metadata is assigned.
      auto state = std::vector<table_slice>{};
      state.push_back(subslice(slice, begin, end));
      begin = end;
      auto new_state = std::vector<table_slice>{};
      for (auto [assignment, value] :
           detail::zip_equal(assignments_, values_slice)) {
        auto begin = int64_t{0};
        for (auto& entry : state) {
          auto entry_rows = detail::narrow<int64_t>(entry.rows());
          auto assigned
            = assign(assignment.left, value.slice(begin, entry_rows), entry,
                     ctrl.diagnostics());
          begin += entry_rows;
          new_state.insert(new_state.end(),
                           std::move_iterator{assigned.begin()},
                           std::move_iterator{assigned.end()});
        }
        std::swap(state, new_state);
        new_state.clear();
      }
      std::ranges::move(state, std::back_inserter(results));
    }
    // TODO: Consider adding a property to function plugins that let's them
    // indicate whether they want their outputs to be strictly ordered. If any
    // of the called functions has this requirement, then we should not be
    // making this optimization. This will become relevant in the future once we
    // allow functions to be stateful.
    if (order_ != event_order::ordered) {
      std::ranges::stable_sort(results, std::ranges::less{},
                               &table_slice::schema);
      results = rebatch_events(std::move(results));
    }
    for (auto& result : results) {
      co_yield std::move(result);
    }
  }
}

} // namespace tenzir
