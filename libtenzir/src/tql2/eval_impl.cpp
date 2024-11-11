//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/detail/enumerate.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/eval_impl.hpp>

#include <ranges>

namespace tenzir {

auto evaluator::eval(const ast::record& x) -> series {
  auto fields = detail::stable_map<std::string, series>{};
  for (auto& item : x.items) {
    item.match(
      [&](const ast::record::field& field) {
        auto val = eval(field.expr);
        fields[field.name.name] = std::move(val);
      },
      [&](const ast::spread& spread) {
        auto val = eval(spread.expr);
        auto rec = val.as<record_type>();
        if (not rec) {
          diagnostic::warning("expected record, got {}", val.type.kind())
            .primary(spread.expr)
            .emit(ctx_);
          return;
        }
        for (auto [i, array] : detail::enumerate(rec->array->fields())) {
          auto field = rec->type.field(i);
          fields[field.name] = series{field.type, array};
        }
      });
  }
  auto field_names = fields | std::views::transform([](auto& x) {
                       return x.first;
                     });
  auto field_arrays = fields | std::views::transform([](auto& x) {
                        return x.second.array;
                      });
  auto field_types = fields | std::views::transform([](auto& x) {
                       return record_type::field_view{x.first, x.second.type};
                     });
  auto result
    = make_struct_array(length_, nullptr,
                        std::vector(field_names.begin(), field_names.end()),
                        std::vector(field_arrays.begin(), field_arrays.end()));
  return series{
    type{record_type{std::vector(field_types.begin(), field_types.end())}},
    std::move(result),
  };
}

auto evaluator::eval(const ast::list& x) -> series {
  using result_t = variant<series, basic_series<list_type>>;
  auto results = std::vector<result_t>{};
  auto item_ty = type{null_type{}};
  for (auto& item : x.items) {
    item.match(
      [&](const ast::expression& expr) {
        auto array = eval(expr);
        auto unified = unify(item_ty, array.type);
        if (unified) {
          item_ty = std::move(*unified);
          results.emplace_back(std::move(array));
        } else {
          auto diag
            = diagnostic::warning("type clash in list, using `null` instead")
                .primary(expr);
          if (item_ty.kind() != array.type.kind()) {
            diag = std::move(diag).note("expected `{}` but got `{}`",
                                        item_ty.kind(), array.type.kind());
          }
          std::move(diag).emit(ctx_);
          results.emplace_back(series::null(null_type{}, length_));
        }
      },
      [&](const ast::spread& spread) {
        auto array = eval(spread.expr);
        auto list = array.as<list_type>();
        if (not list) {
          diagnostic::warning("expected list, got `{}` instead",
                              array.type.kind())
            .primary(spread.expr)
            .emit(ctx_);
          return;
        }
        auto value_ty = list->type.value_type();
        auto unified = unify(item_ty, value_ty);
        if (unified) {
          item_ty = std::move(*unified);
          results.emplace_back(std::move(*list));
        } else {
          auto diag
            = diagnostic::warning("type clash in list, discarding items")
                .primary(spread.expr);
          if (item_ty.kind() != value_ty.kind()) {
            diag = std::move(diag).note("expected `{}` but got `{}`",
                                        item_ty.kind(), value_ty.kind());
          }
          std::move(diag).emit(ctx_);
        }
      });
  }
  // TODO: Rewrite this, `series_builder` is probably not the right tool.
  auto b = series_builder{type{list_type{item_ty}}};
  for (auto row = int64_t{0}; row < length_; ++row) {
    auto l = b.list();
    for (auto& result : results) {
      // TODO: This is not very performant.
      result.match(
        [&](const series& s) {
          l.data(value_at(s.type, *s.array, row));
        },
        [&](const basic_series<list_type>& s) {
          auto& values = s.array->values();
          auto value_ty = s.type.value_type();
          auto begin = s.array->value_offset(row);
          auto end = s.array->value_offset(row + 1);
          for (auto i = begin; i < end; ++i) {
            l.data(value_at(value_ty, *values, i));
          }
        });
    }
  }
  return b.finish_assert_one_array();
}

auto evaluator::eval(const ast::field_access& x) -> series {
  auto l = eval(x.left);
  if (auto null = l.as<null_type>()) {
    return std::move(*null);
  }
  auto rec_ty = try_as<record_type>(l.type);
  if (not rec_ty) {
    diagnostic::warning("cannot access field of non-record type")
      .primary(x.name)
      .secondary(x.left, "type `{}`", l.type.kind())
      .emit(ctx_);
    return null();
  }
  auto& s = as<arrow::StructArray>(*l.array);
  for (auto [i, field] : detail::enumerate<int>(rec_ty->fields())) {
    if (field.name == x.name.name) {
      auto has_null = s.null_count() != 0;
      if (has_null) {
        // TODO: It's not 100% obvious that we want to have this warning, but we
        // went with it for now. Note that this can create cascading warnings.
        diagnostic::warning("tried to access field of `null`")
          .primary(x.name)
          .emit(ctx_);
        return series{field.type, check(s.GetFlattenedField(i))};
      }
      return series{field.type, s.field(i)};
    }
  }
  diagnostic::warning("record does not have this field")
    .primary(x.name)
    .emit(ctx_);
  return null();
}

auto evaluator::eval(const ast::function_call& x) -> series {
  // TODO: We parse the function call every time we get a new batch here. We
  // could store the result in the AST if that becomes a problem, but that is
  // also not an optimal solution.
  auto func
    = ctx_.reg().get(x).make_function(function_plugin::invocation{x}, ctx_);
  if (not func) {
    return series::null(null_type{}, length_);
  }
  auto result = (*func)->run(function_use::evaluator{this}, ctx_);
  TENZIR_ASSERT(result.length() == length_);
  return result;
}

auto evaluator::eval(const ast::this_& x) -> series {
  auto& input = input_or_throw(x);
  return {input.schema(), to_record_batch(input)->ToStructArray().ValueOrDie()};
}

auto evaluator::eval(const ast::root_field& x) -> series {
  auto& input = input_or_throw(x);
  auto& rec_ty = as<record_type>(input.schema());
  for (auto [i, field] : detail::enumerate<int>(rec_ty.fields())) {
    if (field.name == x.ident.name) {
      // TODO: Is this correct?
      return series{field.type, to_record_batch(input)->column(i)};
    }
  }
  diagnostic::warning("field `{}` not found", x.ident.name)
    .primary(x.ident)
    .emit(ctx_);
  return null();
}

auto evaluator::eval(const ast::index_expr& x) -> series {
  if (auto constant = std::get_if<ast::constant>(&*x.index.kind)) {
    // TODO: Generalize this and simplify.
    if (auto string = std::get_if<std::string>(&constant->value)) {
      return eval(ast::field_access{
        x.expr,
        x.lbracket,
        ast::identifier{*string, constant->source},
      });
    }
  }
  auto value = eval(x.expr);
  if (auto null = value.as<null_type>()) {
    return std::move(*null);
  }
  auto index = eval(x.index);
  if (auto null = index.as<null_type>()) {
    return std::move(*null);
  }
  if (auto number = index.as<int64_type>()) {
    auto list = value.as<list_type>();
    if (not list) {
      diagnostic::warning("cannot index into `{}` with `{}`", value.type.kind(),
                          index.type.kind())
        .primary(x.index)
        .emit(ctx_);
      return null();
    }
    auto list_values = list->array->values();
    auto value_type = list->type.value_type();
    auto b = value_type.make_arrow_builder(arrow::default_memory_pool());
    check(b->Reserve(list->length()));
    auto out_of_bounds = false;
    auto list_null = false;
    auto number_null = false;
    for (auto i = int64_t{0}; i < list->length(); ++i) {
      if (not list->array->IsValid(i)) {
        list_null = true;
        check(b->AppendNull());
        continue;
      }
      if (not number->array->IsValid(i)) {
        number_null = true;
        check(b->AppendNull());
        continue;
      }
      auto target = number->array->Value(i);
      auto length = list->array->value_length(i);
      if (target < 0) {
        target = length + target;
      }
      if (target < 0 || target >= length) {
        out_of_bounds = true;
        check(b->AppendNull());
        continue;
      }
      auto offset = list->array->value_offset(i);
      auto value_index = offset + target;
      check(append_array_slice(*b, value_type, *list_values, value_index, 1));
    }
    if (out_of_bounds) {
      diagnostic::warning("list index out of bounds")
        .primary(x.index)
        .emit(ctx_);
    }
    if (list_null) {
      diagnostic::warning("cannot index into `null`").primary(x.expr).emit(ctx_);
    }
    if (number_null) {
      diagnostic::warning("cannot use `null` as index")
        .primary(x.index)
        .emit(ctx_);
    }
    return series{value_type, finish(*b)};
  }
  return not_implemented(x);
}

auto evaluator::eval(const ast::meta& x) -> series {
  auto& input = input_or_throw(x);
  switch (x.kind) {
    case ast::meta::name:
      return to_series(std::string{input.schema().name()});
    case ast::meta::import_time: {
      auto result = input.import_time();
      if (result == time{}) {
        return series::null(time_type{}, length_);
      }
      return to_series(result);
    }
    case ast::meta::internal:
      return to_series(input.schema().attribute("internal").has_value());
  }
  TENZIR_UNREACHABLE();
}

auto evaluator::eval(const ast::assignment& x) -> series {
  // TODO: What shall happen if we hit this in const eval mode?
  diagnostic::warning("unexpected assignment").primary(x).emit(ctx_);
  return null();
}

auto evaluator::eval(const ast::constant& x) -> series {
  return to_series(x.as_data());
}

auto evaluator::eval(const ast::expression& x) -> series {
  return x.match([&](auto& y) {
    return eval(y);
  });
}

auto evaluator::input_or_throw(into_location location) -> const table_slice& {
  if (not input_) {
    diagnostic::error("expected a constant expression")
      .primary(location)
      .emit(ctx_);
    throw failure::promise();
  }
  return *input_;
}

auto evaluator::to_series(const data& x) const -> series {
  // TODO: This is overkill.
  auto b = series_builder{};
  for (auto i = int64_t{0}; i < length_; ++i) {
    b.data(x);
  }
  return b.finish_assert_one_array();
}

} // namespace tenzir
