//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval_impl.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/to_string.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/view3.hpp"

#include <limits>
#include <ranges>

namespace tenzir {

/// Find the best matching field name from available field names.
auto suggest_field_name(std::string_view requested_field,
                        const record_type* rec)
  -> std::optional<std::string_view> {
  auto best_field = std::string_view{};
  auto best_similarity = std::numeric_limits<int64_t>::min();
  for (const auto& field : rec->fields()) {
    if (field.name.empty()) {
      continue;
    }
    const auto similarity
      = detail::calculate_similarity(requested_field, field.name);
    if (similarity > best_similarity) {
      best_similarity = similarity;
      best_field = field.name;
    }
  }
  if (best_field.empty() or best_similarity <= -3) {
    return std::nullopt;
  }
  return best_field;
}

auto evaluator::eval(const ast::record& x) -> multi_series {
  auto arrays = std::vector<multi_series>{};
  for (const auto& item : x.items) {
    arrays.push_back(eval(item.match(
      [&](const ast::record::field& field) {
        return field.expr;
      },
      [&](const ast::spread& spread) {
        return spread.expr;
      })));
  }
  return map_series(arrays, [&](std::span<series> arrays) {
    auto length = arrays.empty() ? length_ : arrays.front().length();
    auto fields = detail::stable_map<std::string, series>{};
    for (auto [array, item] : detail::zip_equal(arrays, x.items)) {
      match(
        item,
        [&](const ast::record::field& field) {
          fields[field.name.name] = std::move(array);
        },
        [&](const ast::spread& spread) {
          auto records = array.as<record_type>();
          if (not records) {
            if (array.type.kind().is_not<null_type>()) {
              diagnostic::warning("expected record, got {}", array.type.kind())
                .primary(spread.expr)
                .emit(ctx_);
            }
            return;
          }
          // TODO: We could also make sure that record-nulls do not create any
          // fields. This will create an additional splitting point.
          for (auto [i, field_array] :
               detail::enumerate(check(records->array->Flatten()))) {
            auto field = records->type.field(i);
            fields[field.name] = series{field.type, field_array};
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
    auto new_type
      = type{record_type{std::vector(field_types.begin(), field_types.end())}};
    auto result
      = make_struct_array(length, nullptr,
                          std::vector(field_names.begin(), field_names.end()),
                          std::vector(field_arrays.begin(), field_arrays.end()),
                          as<record_type>(new_type));
    return series{
      std::move(new_type),
      std::move(result),
    };
  });
}

auto evaluator::eval(const ast::list& x) -> multi_series {
  if (x.items.empty()) {
    auto b = series_builder{};
    for (auto i = int64_t{0}; i < length_; ++i) {
      b.list();
    }
    return b.finish_assert_one_array();
  }
  auto arrays = std::vector<multi_series>{};
  for (const auto& item : x.items) {
    arrays.push_back(match(
      item,
      [&](const ast::expression& expr) {
        return eval(expr);
      },
      [&](const ast::spread& spread) {
        return eval(spread.expr);
      }));
  }
  return map_series(arrays, [&](std::span<series> arrays) -> series {
    using result_t = variant<series, basic_series<list_type>>;
    const auto slice_length = arrays.front().length();
    auto results = std::vector<result_t>{};
    auto value_type = type{null_type{}};
    for (auto [array, item] : detail::zip_equal(arrays, x.items)) {
      item.match(
        [&](const ast::expression& expr) {
          auto unified = unify(value_type, array.type);
          if (unified) {
            value_type = std::move(*unified);
            results.emplace_back(std::move(array));
          } else {
            auto diag
              = diagnostic::warning("type clash in list, using `null` instead")
                  .primary(expr);
            if (value_type.kind() != array.type.kind()) {
              diag = std::move(diag).note("expected `{}` but got `{}`",
                                          value_type.kind(), array.type.kind());
            }
            std::move(diag).emit(ctx_);
            results.emplace_back(series::null(null_type{}, slice_length));
          }
        },
        [&](const ast::spread& spread) {
          auto list = array.as<list_type>();
          if (not list) {
            diagnostic::warning("expected list, got `{}` instead",
                                array.type.kind())
              .primary(spread.expr)
              .emit(ctx_);
            return;
          }
          auto value_ty = list->type.value_type();
          auto unified = unify(value_type, value_ty);
          if (unified) {
            value_type = std::move(*unified);
            results.emplace_back(std::move(*list));
          } else {
            auto diag
              = diagnostic::warning("type clash in list, discarding items")
                  .primary(spread.expr);
            if (value_type.kind() != value_ty.kind()) {
              diag = std::move(diag).note("expected `{}` but got `{}`",
                                          value_type.kind(), value_ty.kind());
            }
            std::move(diag).emit(ctx_);
          }
        });
    }
    // TODO: Rewrite this, `series_builder` is probably not the right tool.
    auto b = series_builder{type{list_type{value_type}}};
    for (auto row = int64_t{0}; row < slice_length; ++row) {
      auto l = b.list();
      for (auto& result : results) {
        // TODO: This is not very performant.
        result.match(
          [&](const series& s) {
            l.data(value_at(s.type, *s.array, row));
          },
          [&](const basic_series<list_type>& s) {
            const auto& values = s.array->values();
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
  });
}

auto evaluator::eval(const ast::field_access& x) -> multi_series {
  return map_series(eval(x.left), [&](series l) -> series {
    if (auto null = l.as<null_type>()) {
      if (not x.suppress_warnings()) {
        diagnostic::warning("tried to access field of `null`")
          .primary(x.name)
          .hint("append `?` to suppress this warning")
          .emit(ctx_);
      }
      return std::move(*null);
    }
    auto* rec_ty = try_as<record_type>(l.type);
    if (not rec_ty) {
      diagnostic::warning("cannot access field of non-record type")
        .primary(x.name)
        .secondary(x.left, "type `{}`", l.type.kind())
        .emit(ctx_);
      return null();
    }
    auto& s = as<arrow::StructArray>(*l.array);
    if (auto idx = rec_ty->resolve_field(x.name.name)) {
      const auto has_null = s.null_count() != 0;
      if (has_null and not x.suppress_warnings()) {
        diagnostic::warning("tried to access field of `null`")
          .primary(x.name)
          .hint("append `?` to suppress this warning")
          .emit(ctx_);
        return series{
          rec_ty->field(*idx).type,
          check(s.GetFlattenedField(detail::narrow<int>(*idx))),
        };
      }
      return series{
        rec_ty->field(*idx).type,
        s.field(detail::narrow<int>(*idx)),
      };
    }
    if (not x.suppress_warnings()) {
      diagnostic::warning("record does not have this field")
        .primary(x.name)
        .compose([&](auto&& d) {
          auto suggestion = suggest_field_name(x.name.name, rec_ty);
          return suggestion
                   ? std::move(d).hint("did you mean `{}`?", *suggestion)
                   : d;
        })
        .hint(std::string{"append `?` to suppress this warning"})
        .emit(ctx_);
    }
    return null();
  });
}

auto evaluator::eval(const ast::function_call& x) -> multi_series {
  // TODO: We parse the function call every time we get a new batch here. We
  // could store the result in the AST if that becomes a problem, but that is
  // also not an optimal solution.
  auto func
    = ctx_.reg().get(x).make_function(function_plugin::invocation{x}, ctx_);
  if (not func) {
    return series::null(null_type{}, length_);
  }
  return (*func)->run(function_use::evaluator{this}, ctx_);
}

auto evaluator::eval(const ast::this_& x) -> multi_series {
  const auto& input = input_or_throw(x);
  return series{input.schema(), check(to_record_batch(input)->ToStructArray())};
}

auto evaluator::eval(const ast::root_field& x) -> multi_series {
  const auto& input = input_or_throw(x);
  const auto& rec_ty = as<record_type>(input.schema());
  if (auto idx = rec_ty.resolve_field(x.id.name)) {
    return series{
      rec_ty.field(*idx).type,
      to_record_batch(input)->column(detail::narrow<int>(*idx)),
    };
  }
  if (not x.has_question_mark) {
    diagnostic::warning("field `{}` not found", x.id.name)
      .primary(x.id)
      .compose([&](auto&& d) {
        auto suggestion = suggest_field_name(x.id.name, &rec_ty);
        return suggestion ? std::move(d).hint("did you mean `{}`?", *suggestion)
                          : d;
      })
      .hint(std::string{"append `?` to suppress this warning"})
      .emit(ctx_);
  }
  return null();
}

auto evaluator::eval(const ast::index_expr& x) -> multi_series {
  // We map `foo["bar"]` onto the implementation of `foo.bar`, as that has a
  // faster implementation that is optimized for the accessed field name being a
  // constant.
  if (const auto* constant = try_as<ast::constant>(x.index)) {
    if (const auto* string = try_as<std::string>(constant->value)) {
      return eval(ast::field_access{
        x.expr,
        location::unknown,
        x.has_question_mark,
        ast::identifier{*string, constant->source},
      });
    }
  }
  return map_series(
    eval(x.expr), eval(x.index),
    [&](series value, const series& index) -> multi_series {
      const auto add_suppress_hint = [&](auto diag) {
        // The `get` function internally creates an `ast::index_expr` and
        // evaluates it. We change the warning when it is used.
        return std::move(diag).hint(
          x.rbracket != location::unknown
            ? "use `[…]?` to suppress this warning"
            : "provide a fallback value to suppress this warning");
      };
      TENZIR_ASSERT(value.length() == index.length());
      if (auto null = value.as<null_type>()) {
        if (not x.has_question_mark) {
          diagnostic::warning("tried to access field of `null`")
            .primary(x.expr, "is null")
            .compose(add_suppress_hint)
            .emit(ctx_);
        }
        return std::move(*null);
      }
      if (auto null = index.as<null_type>()) {
        if (not x.has_question_mark) {
          diagnostic::warning("cannot use `null` as index")
            .primary(x.index, "is null")
            .compose(add_suppress_hint)
            .emit(ctx_);
        }
        return std::move(*null);
      }
      if (auto str = index.as<string_type>()) {
        auto* ty = try_as<record_type>(value.type);
        if (not ty) {
          diagnostic::warning("cannot access field of non-record type")
            .primary(x.index)
            .secondary(x.expr, "has type `{}`", value.type.kind())
            .emit(ctx_);
          return null();
        }
        auto& s = as<arrow::StructArray>(*value.array);
        auto b = series_builder{};
        auto result = std::vector<series>{};
        auto last_type = type{null_type{}};
        auto not_found = std::vector<std::string>{};
        auto field_map = detail::heterogeneous_string_hashmap<series>{};
        auto warn_null_record = false;
        auto warn_null_index = false;
        for (auto [i, field] : detail::enumerate<int>(ty->fields())) {
          auto [_, inserted] = field_map.try_emplace(std::string{field.name},
                                                     field.type, s.field(i));
          TENZIR_ASSERT(inserted);
        }
        for (auto i = int64_t{}; i < s.length(); ++i) {
          if (s.IsNull(i)) {
            b.null();
            warn_null_record = true;
            continue;
          }
          if (str->array->IsNull(i)) {
            warn_null_index = true;
            b.null();
            continue;
          }
          auto name = value_at(string_type{}, *str->array, i);
          if (auto it = field_map.find(name); it != field_map.end()) {
            const auto& field = it->second;
            if (field.type.kind().is_not<null_type>()
                and field.type != last_type) {
              if (last_type.kind().is_not<null_type>()) {
                result.push_back(b.finish_assert_one_array());
              }
              last_type = field.type;
            }
            auto v = value_at(field.type, *field.array, i);
            b.data(v);
          } else {
            if (std::ranges::find(not_found, name) == not_found.end()) {
              if (not x.has_question_mark) {
                diagnostic::warning("record does not have field `{}`", name)
                  .primary(x.index, "does not exist")
                  .compose([&](auto&& d) {
                    auto suggestion = suggest_field_name(name, ty);
                    return suggestion ? std::move(d).hint("did you mean `{}`?",
                                                          *suggestion)
                                      : d;
                  })
                  .hint("use `[…]?` to suppress this warning")
                  .emit(ctx_);
              }
              not_found.emplace_back(name);
            }
            b.null();
          }
        }
        if (warn_null_record and not x.has_question_mark) {
          diagnostic::warning("tried to access field of `null`")
            .primary(x.expr)
            .compose(add_suppress_hint)
            .emit(ctx_);
        }
        if (warn_null_index and not x.has_question_mark) {
          diagnostic::warning("cannot use `null` as index")
            .primary(x.index, "is null")
            .compose(add_suppress_hint)
            .emit(ctx_);
        }
        result.push_back(b.finish_assert_one_array());
        return multi_series{result};
      }
      if (auto number = index.as<int64_type>()) {
        if (auto record = value.as<record_type>()) {
          auto result = multi_series{};
          auto warn_null_index = false;
          auto warn_index_out_of_bounds = false;
          auto last_field = std::optional<int64_t>{};
          auto group_offset = int64_t{};
          const auto add = [&](int64_t begin, int64_t end) {
            if (begin == end) {
              return;
            }
            if (not last_field) {
              result.append(series::null(null_type{}, end - begin));
              return;
            }
            if (*last_field < 0 or *last_field >= record->array->num_fields()) {
              warn_index_out_of_bounds = true;
              result.append(series::null(null_type{}, end - begin));
              return;
            }

            result.append(series{
              record->type.field(*last_field).type,
              check(record->array->field(detail::narrow_cast<int>(*last_field))
                      ->SliceSafe(begin, end - begin)),
            });
          };
          for (auto i = int64_t{}; i < number->length(); ++i) {
            if (number->array->IsNull(i)) {
              if (not last_field) {
                continue;
              }
              warn_null_index = true;
              add(group_offset, i);
              last_field.reset();
              group_offset = i;
              continue;
            }
            const auto field = number->array->GetView(i);
            if (field == last_field) {
              continue;
            }
            add(group_offset, i);
            last_field = field;
            group_offset = i;
          }
          add(group_offset, number->length());
          if (warn_null_index and not x.has_question_mark) {
            diagnostic::warning("cannot use `null` as index")
              .primary(x.index, "is null")
              .compose(add_suppress_hint)
              .emit(ctx_);
          }
          if (warn_index_out_of_bounds and not x.has_question_mark) {
            diagnostic::warning("index out of bounds")
              .primary(x.index, "is out of bounds")
              .compose(add_suppress_hint)
              .emit(ctx_);
          }
          return result;
        }
        auto list = value.as<list_type>();
        if (not list) {
          if (not is<null_type>(value.type) or not x.has_question_mark) {
            diagnostic::warning("expected `record` or `list`")
              .primary(x.expr, "has type `{}`", value.type.kind())
              .compose(add_suppress_hint)
              .emit(ctx_);
          }
          return null();
        }
        auto list_values = list->array->values();
        auto value_type = list->type.value_type();
        auto b = value_type.make_arrow_builder(arrow_memory_pool());
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
          check(
            append_array_slice(*b, value_type, *list_values, value_index, 1));
        }
        if (out_of_bounds and not x.has_question_mark) {
          diagnostic::warning("list index out of bounds")
            .primary(x.index, "is out of bounds")
            .compose(add_suppress_hint)
            .emit(ctx_);
        }
        if (list_null and not x.has_question_mark) {
          diagnostic::warning("cannot index into `null`")
            .primary(x.expr, "is null")
            .compose(add_suppress_hint)
            .emit(ctx_);
        }
        if (number_null and not x.has_question_mark) {
          diagnostic::warning("cannot use `null` as index")
            .primary(x.index, "is null")
            .compose(add_suppress_hint)
            .emit(ctx_);
        }
        return series{value_type, finish(*b)};
      }
      return not_implemented(x);
    });
}

auto evaluator::eval(const ast::meta& x) -> multi_series {
  const auto& input = input_or_throw(x);
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

auto evaluator::eval(const ast::assignment& x) -> multi_series {
  // TODO: What shall happen if we hit this in const eval mode?
  diagnostic::warning("unexpected assignment").primary(x).emit(ctx_);
  return null();
}

auto evaluator::eval(const ast::constant& x) -> multi_series {
  return to_series(x.as_data());
}

auto evaluator::eval(const ast::format_expr& x) -> multi_series {
  auto cols = std::vector<variant<std::string, multi_series>>{};
  cols.reserve(x.segments.size());
  for (const auto& s : x.segments) {
    match(
      s,
      [&](const std::string& s) {
        cols.emplace_back(s);
      },
      [&](const ast::format_expr::replacement& r) {
        auto ms = eval(r.expr);
        for (auto& part : ms) {
          part = resolve_enumerations(std::move(part));
        }
        cols.emplace_back(std::move(ms));
      });
  }
  auto res = multi_series{};
  auto current_builder = std::variant<type_to_arrow_builder_t<string_type>,
                                      type_to_arrow_builder_t<secret_type>>{};
  const auto append_builder_to_result = detail::overload{
    [&res](type_to_arrow_builder_t<string_type>& b) {
      res.append(series{string_type{}, finish(b)});
    },
    [&res](type_to_arrow_builder_t<secret_type>& b) {
      res.append(series{secret_type{}, finish(b)});
    },
  };
  const auto append_row_to_builder = detail::overload{
    [&current_builder, &append_builder_to_result](const std::string& str) {
      if (auto* str_builder
          = try_as<type_to_arrow_builder_t<string_type>>(current_builder)) {
        check(append_builder(string_type{}, *str_builder, str));
      } else if (auto* sec_builder
                 = try_as<type_to_arrow_builder_t<secret_type>>(
                   current_builder)) {
        if (sec_builder->length() > 0) {
          append_builder_to_result(*sec_builder);
        }
        auto& new_builder
          = current_builder.emplace<type_to_arrow_builder_t<string_type>>();
        check(append_builder(string_type{}, new_builder, str));
      }
    },
    [&current_builder, &append_builder_to_result](const secret& sec) {
      if (auto* sec_builder
          = try_as<type_to_arrow_builder_t<secret_type>>(current_builder)) {
        check(append_builder(secret_type{}, *sec_builder, sec));
      } else if (auto* str_builder
                 = try_as<type_to_arrow_builder_t<string_type>>(
                   current_builder)) {
        if (str_builder->length() > 0) {
          append_builder_to_result(*str_builder);
        }
        auto& new_builder
          = current_builder.emplace<type_to_arrow_builder_t<secret_type>>();
        check(append_builder(secret_type{}, new_builder, sec));
      }
    },
  };
  for (auto i = int64_t{0}; i < length_; ++i) {
    auto row = std::variant<std::string, secret>{};
    const auto add_column_to_row = detail::overload{
      [&row](const std::string_view& str) {
        if (auto* str_row = try_as<std::string>(row)) {
          str_row->append(str);
        } else if (auto* sec_row = try_as<secret>(row)) {
          *sec_row = sec_row->with_appended(str);
        } else {
          TENZIR_UNREACHABLE();
        }
      },
      [&row](const secret_view& sec) {
        if (auto* str_row = try_as<std::string>(row)) {
          if (str_row->empty()) {
            row = materialize(sec);
          } else {
            row = secret::make_literal(*str_row).with_appended(sec);
          }
        } else if (auto* sec_row = try_as<secret>(row)) {
          *sec_row = sec_row->with_appended(sec);
        } else {
          TENZIR_UNREACHABLE();
        }
      },
    };
    for (auto& c : cols) {
      match(
        c,
        [&add_column_to_row](const std::string& s) {
          add_column_to_row(s);
        },
        [this, &add_column_to_row, i](const multi_series& ms) {
          const auto v = ms.value_at(i);
          if (const auto* sec = try_as<view<secret>>(v)) {
            add_column_to_row(*sec);
            return;
          }
          auto str = to_string(v, location::unknown, ctx_);
          add_column_to_row(str ? *str : "null");
        });
    }
    match(row, append_row_to_builder);
    if (auto* str = try_as<std::string>(row)) {
      str->clear();
    } else {
      row.emplace<std::string>();
    }
  }
  match(current_builder, append_builder_to_result);
  return res;
}

auto evaluator::eval(const ast::lambda_expr& x,
                     const basic_series<list_type>& input) -> multi_series {
  if (const auto* slice = this->get_input()) {
    TENZIR_ASSERT(std::cmp_equal(slice->rows(), input.length()));
    return tenzir::eval(x, input, *slice, ctx_);
  }
  return tenzir::eval(x, input, ctx_);
}

auto evaluator::eval(const ast::expression& x) -> multi_series {
  return trace_panic(x, [&] {
    auto result = x.match([&](auto& y) {
      return eval(y);
    });
    TENZIR_ASSERT(result.length() == length_,
                  "got length {} instead of {} while evaluating {:?}",
                  result.length(), length_, x);
    return result;
  });
}

auto evaluator::input_or_throw(into_location location) -> const table_slice& {
  return input_.match(
    [&](const table_slice* input) -> const table_slice& {
      if (not input) {
        diagnostic::error("expected a constant expression")
          .primary(location)
          .emit(ctx_);
        throw failure::promise();
      }
      return *input;
    },
    [](const table_slice& input) -> const table_slice& {
      return input;
    });
}

auto evaluator::to_series(const data& x) const -> series {
  return data_to_series(x, length_);
}

} // namespace tenzir
