//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/set.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/detail/operators.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api_scalar.h>
#include <caf/detail/is_complete.hpp>
#include <caf/detail/is_one_of.hpp>

#include <ranges>
#include <type_traits>

namespace tenzir::tql2 {

#define TRY(name, expr)                                                        \
  auto _tmp = (expr);                                                          \
  if (auto err = std::get_if<1>(&_tmp)) {                                      \
    return std::move(*err);                                                    \
  }                                                                            \
  name = *std::get_if<0>(&_tmp)

auto resolve(const ast::selector& sel, const table_slice& slice)
  -> variant<series, resolve_error> {
  TRY(auto offset, resolve(sel, slice.schema()));
  auto [ty, array] = offset.get(slice);
  return series{ty, array};
}

auto resolve(const ast::selector& sel, type ty)
  -> variant<offset, resolve_error> {
  // TODO: Write this properly.
  auto sel_index = size_t{0};
  auto result = offset{};
  result.reserve(sel.path.size());
  while (sel_index < sel.path.size()) {
    auto rty = caf::get_if<record_type>(&ty);
    if (not rty) {
      // TODO
      TENZIR_ASSERT(sel_index > 0);
      return resolve_error{sel.path[sel_index - 1],
                           resolve_error::not_a_record{ty}};
    }
    auto found = false;
    auto field_index = size_t{0};
    for (auto&& field : rty->fields()) {
      if (field.name == sel.path[sel_index].name) {
        ty = field.type;
        found = true;
        sel_index += 1;
        break;
      }
      ++field_index;
    }
    if (not found) {
      return resolve_error{sel.path[sel_index],
                           resolve_error::field_not_found{}};
    }
    result.push_back(field_index);
  }
  return result;
}

namespace {

using value = variant<data, series>;

auto value_to_series(const value& val, int64_t length) -> series {
  return val.match(
    [&](const data& x) {
      // TODO: This is overkill.
      auto b = series_builder{};
      for (auto i = int64_t{0}; i < length; ++i) {
        b.data(x);
      }
      return b.finish_assert_one_array();
    },
    [&](const series& x) {
      TENZIR_ASSERT(x.length() == length);
      return x;
    });
}

// TODO: not good.
template <class T>
concept Addable = requires(T x, T y) {
  { x + y } -> std::same_as<T>;
};

template <class T>
concept numeric_type
  = std::same_as<T, int64_type> || std::same_as<T, uint64_type>
    || std::same_as<T, double_type>;

template <ast::binary_op Op, concrete_type L, concrete_type R>
struct EvalBinOp;

template <numeric_type L, numeric_type R>
struct EvalBinOp<ast::binary_op::add, L, R> {
  // double + (u)int -> double
  // uint + int -> int? uint?

  static auto
  eval(const type_to_arrow_array_t<L>& l, const type_to_arrow_array_t<R>& r)
    -> std::shared_ptr<arrow::Array> {
    // if constexpr (std::same_as<L, double_type>
    //               || std::same_as<R, double_type>) {
    //   // double
    // } else {
    //   // if both uint -> uint
    //   // otherwise int?
    // }
    // TODO: Do we actually want to use this?
    // For example, range error leads to no result at all.
    auto opts = arrow::compute::ArithmeticOptions{};
    opts.check_overflow = true;
    auto res = arrow::compute::Add(l, r, opts).ValueOrDie();
    TENZIR_ASSERT(res.is_array());
    return res.make_array();
  }
};

template <>
struct EvalBinOp<ast::binary_op::add, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r)
    -> std::shared_ptr<arrow::Array> {
    auto b = arrow::StringBuilder{};
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (l.IsNull(i) || r.IsNull(i)) {
        (void)b.AppendNull();
        continue;
      }
      auto lv = l.GetView(i);
      auto rv = r.GetView(i);
      (void)b.Append(lv);
      (void)b.ExtendCurrent(rv);
    }
    return b.Finish().ValueOrDie();
  }
};

template <numeric_type L, numeric_type R>
struct EvalBinOp<ast::binary_op::mul, L, R> {
  static auto
  eval(const type_to_arrow_array_t<L>& l, const type_to_arrow_array_t<R>& r)
    -> std::shared_ptr<arrow::Array> {
    auto opts = arrow::compute::ArithmeticOptions{};
    opts.check_overflow = true;
    auto res = arrow::compute::Multiply(l, r, opts).ValueOrDie();
    TENZIR_ASSERT(res.is_array());
    return res.make_array();
  }
};

class evaluator {
public:
  explicit evaluator(const table_slice& input, diagnostic_handler& dh)
    : input_{input}, dh_{dh} {
  }

  auto to_series(const value& val) -> series {
    return value_to_series(val, detail::narrow<int64_t>(input_.rows()));
  }

  auto eval(const ast::expression& x) -> value {
    return x.match([&](auto& y) {
      return eval(y);
    });
  }

  auto eval(const ast::literal& x) -> value {
    return x.as_data();
  }

  auto eval(const ast::record& x) -> value {
    // TODO: Soooo bad.
    auto fields = detail::stable_map<std::string, series>{};
    for (auto& item : x.content) {
      item.match(
        [&](const ast::record::field& field) {
          auto val = to_series(eval(field.expr));
          auto [_, inserted] = fields.emplace(field.name.name, std::move(val));
          if (not inserted) {
            diagnostic::warning("todo: overwrite existing?")
              .primary(field.name.location)
              .emit(dh_);
          }
        },
        [](const ast::record::spread&) {
          TENZIR_TODO();
        });
    }
    auto field_names = fields | std::ranges::views::transform([](auto& x) {
                         return x.first;
                       });
    auto field_arrays = fields | std::ranges::views::transform([](auto& x) {
                          return x.second.array;
                        });
    auto field_types = fields | std::ranges::views::transform([](auto& x) {
                         return record_type::field_view{x.first, x.second.type};
                       });
    auto result = make_struct_array(
      detail::narrow<int64_t>(input_.rows()), nullptr,
      std::vector(field_names.begin(), field_names.end()),
      std::vector(field_arrays.begin(), field_arrays.end()));
    return series{
      type{record_type{std::vector(field_types.begin(), field_types.end())}},
      std::move(result),
    };
  }

  auto eval(const ast::list& x) -> value {
    // [a, b]
    //
    // {a: 1, b: 2}
    // {a: 3, b: 4}
    //
    // <1, 2, 3, 4>
    // |^^^^|
    //       |^^^^|
    auto arrays = std::vector<series>{};
    for (auto& item : x.items) {
      auto array = to_series(eval(item));
      if (not arrays.empty()) {
        // TODO:
        TENZIR_ASSERT(array.type == arrays[0].type);
      }
      arrays.push_back(std::move(array));
    }
    // arrays = [<1, 3>, <2, 4>]
    // TODO:
    TENZIR_ASSERT(not arrays.empty());
    auto b = series_builder{type{list_type{arrays[0].type}}};
    for (auto row = int64_t{0}; row < arrays[0].length(); ++row) {
      auto l = b.list();
      for (auto& array : arrays) {
        // TODO: This is not very good.
        l.data(value_at(array.type, *array.array, row));
      }
    }
    return b.finish_assert_one_array();
  }

  auto eval(const ast::selector& x) -> value {
    auto result = resolve(x, input_);
    return result.match(
      [](series& x) -> value {
        return std::move(x);
      },
      [&](resolve_error& err) -> value {
        err.reason.match(
          [&](resolve_error::not_a_record& reason) {
            diagnostic::warning("expected record, found {}", reason.type.kind())
              .primary(err.ident.location)
              .emit(dh_);
          },
          [&](resolve_error::field_not_found&) {
            diagnostic::warning("field `{}` not found", err.ident.name)
              .primary(err.ident.location)
              .emit(dh_);
          });
        return caf::none;
      });
  }

  auto eval(const ast::function_call& x) -> value {
    TENZIR_ASSERT(x.fn.ref.resolved());
    auto segments = x.fn.ref.segments();
    TENZIR_ASSERT(segments.size() == 1);
    auto fn = plugins::find<function_plugin>("tql2." + segments[0]);
    TENZIR_ASSERT(fn);
    auto args = std::vector<series>{};
    for (auto& arg : x.args) {
      args.push_back(to_series(eval(arg)));
    }
    auto ret = fn->eval(x, input_.rows(), std::move(args), dh_);
    TENZIR_ASSERT(ret.length() == detail::narrow<int64_t>(input_.rows()));
    return ret;
  }

  template <ast::binary_op Op>
  auto eval(const ast::binary_expr& x, const series& l, const series& r)
    -> value {
    TENZIR_ASSERT(x.op.inner == Op);
    TENZIR_ASSERT(l.length() == r.length());
    return caf::visit(
      [&]<concrete_type L, concrete_type R>(const L&, const R&) -> value {
        if constexpr (caf::detail::is_complete<EvalBinOp<Op, L, R>>) {
          using LA = type_to_arrow_array_t<L>;
          using RA = type_to_arrow_array_t<R>;
          auto& la = caf::get<LA>(*l.array);
          auto& ra = caf::get<RA>(*r.array);
          auto oa = EvalBinOp<Op, L, R>::eval(la, ra);
          auto ot = type::from_arrow(*oa->type());
          return series{std::move(ot), std::move(oa)};
        } else {
          // TODO: Not possible? Where coercion?
          diagnostic::warning("binary operator `{}` not implemented for `{}` "
                              "and `{}`",
                              x.op.inner, l.type.kind(), r.type.kind())
            .primary(x.op.source)
            .emit(dh_);
          return caf::none;
        }
      },
      l.type, r.type);
  }

  auto eval(const ast::binary_expr& x) -> value {
#if 1
    auto l = to_series(eval(x.left));
    auto r = to_series(eval(x.right));
    switch (x.op.inner) {
      case ast::binary_op::add:
        return eval<ast::binary_op::add>(x, l, r);
      case ast::binary_op::mul:
        return eval<ast::binary_op::mul>(x, l, r);
      default:
        return not_implemented(x);
    }
#else
    if (x.op.inner != ast::binary_op::add) {
      return not_implemented(x);
    }
    auto l = eval(x.left);
    auto r = eval(x.right);
    auto add
      = [&]<bool Swap>(std::bool_constant<Swap>, series& l, data& r) -> value {
      auto f = [&]<class Array, class Data>(const Array& l, Data& r) -> value {
        if constexpr (std::same_as<Data, pattern>) {
          TENZIR_UNREACHABLE();
        } else if constexpr (std::same_as<Data, caf::none_t>) {
          return caf::none;
        } else if constexpr (std::same_as<Array, arrow::NullArray>) {
          return caf::none;
        } else if constexpr (std::same_as<Array, type_to_arrow_array_t<
                                                   data_to_type_t<Data>>>) {
          if constexpr (Addable<Data>) {
            auto ty = data_to_type_t<Data>{};
            auto b = series_builder{type{ty}};
            // TODO: This code is obviously very bad.
            for (auto row = int64_t{0}; row < l.length(); ++row) {
              if (l.IsNull(row)) {
                b.null();
              } else {
                auto val = materialize(value_at(ty, l, row));
                b.data(Swap ? r + val : val + r);
              }
            }
            // for (auto&& lv : l) {
            //   if (lv) {
            //     b.data(*lv + r);
            //   } else {
            //     b.null();
            //   }
            // }
            return b.finish_assert_one_array();
          } else {
            // TODO: Same type, but not addable.
            return caf::none;
          }
        } else {
          // TODO: warn only once?!
          diagnostic::warning("cannot add {} and {}",
                              type_kind::of<data_to_type_t<Data>>, "TODO")
            .primary(x.get_location())
            .emit(dh_);
          return caf::none;
        }
      };
      return caf::visit(f, *l.array, r);
    };
    auto f = detail::overload{
      [](data& l, data& r) -> value {
        // TODO: This is basically const eval.
        return caf::visit(detail::overload{
                            []<Addable T>(T& l, T& r) -> data {
                              return l + r;
                            },
                            [](auto l, auto r) -> data {
                              TENZIR_WARN("not addable: {}",
                                          typeid(decltype(l)).name());
                              TENZIR_UNUSED(l, r);
                              return caf::none;
                            },
                          },
                          l, r);
      },
      [&](series& l, data& r) -> value {
        return add(std::bool_constant<false>{}, l, r);
        // auto l2 =
        // caf::get_if<arrow::Int64Array>(&*l.array); auto
        // r2 = caf::get_if<int64_t>(&r); if (not l2 ||
        // not r2) {
        //   return not_implemented(x);
        // }
        // // TODO
        // auto b = arrow::Int64Builder{};
        // (void)b.Reserve(l.length());
        // for (auto lv : *l2) {
        //   if (lv) {
        //     (void)b.Append(*lv + *r2);
        //   } else {
        //     // TODO: Warn here?
        //     (void)b.AppendNull();
        //   }
        // }
        // auto res =
        // std::shared_ptr<arrow::Int64Array>();
        // (void)b.Finish(&res);
        // return series{int64_type{}, std::move(res)};
      },
      [&](data& l, series& r) {
        return add(std::bool_constant<true>{}, r, l);
      },
      [&](series& l, series& r) -> value {
        return caf::visit(
          [&]<class L, class R>(const L&, const R&) -> value {
            if constexpr (not std::same_as<L, R>) {
              return caf::none;
            } else {
              using Ty = L;
              using Array = type_to_arrow_array_t<Ty>;
              auto& la = caf::get<Array>(*l.array);
              auto& ra = caf::get<Array>(*r.array);
              using Data = type_to_data_t<Ty>;
              if constexpr (Addable<Data>) {
                TENZIR_ASSERT(l.length() == r.length());
                auto ty = Ty{};
                // TODO: Bad bad bad.
                auto b = series_builder{type{ty}};
                for (auto row = int64_t{0}; row < l.length(); ++row) {
                  if (l.array->IsNull(row) || r.array->IsNull(row)) {
                    b.null();
                  } else {
                    auto lv = materialize(value_at(ty, la, row));
                    auto rv = materialize(value_at(ty, ra, row));
                    b.data(lv + rv);
                  }
                }
                return b.finish_assert_one_array();
              } else {
                return caf::none;
              }
            }
          },
          l.type, r.type);
      },
    };
    return std::visit(f, l, r);
#endif
  } // namespace

  auto eval(const auto& x) -> value {
    return not_implemented(x);
  }

  auto not_implemented(const auto& x) -> value {
    diagnostic::warning("eval not implemented yet")
      .primary(x.get_location())
      .emit(dh_);
    return caf::none;
  }

private:
  const table_slice& input_;
  diagnostic_handler& dh_;
}; // namespace tenzir::tql2

} // namespace

auto set_operator::operator()(generator<table_slice> input,
                              operator_control_plane& ctrl) const
  -> generator<table_slice> {
  for (auto&& slice : input) {
    if (slice.rows() == 0) {
      co_yield {};
      continue;
    }
    // 1. Evaluate every right-hand side with the original input.
    // 2. Evaluate every left-hand side as l-value and then assign.
    // => Left side is evaluated after side effects, in order!
    // set foo={bar: 42}, foo.bar=foo.bar+42
    auto result = slice;
    for (auto& assignment : assignments_) {
      // TODO: We are using `slice` here, not `result`. Okay?
      auto res = evaluator{slice, ctrl.diagnostics()}.eval(assignment.right);
      auto s = value_to_series(res, slice.rows());
      auto resolved = resolve(assignment.left, result.schema());
      auto off = std::get_if<offset>(&resolved);
      // TODO: Write this without transform columns.
      auto transformation = indexed_transformation{};
      if (off) {
        transformation = indexed_transformation{
          .index = std::move(*off),
          .fun =
            [&](struct record_type::field field,
                std::shared_ptr<arrow::Array> array) {
              TENZIR_UNUSED(array);
              field.type = std::move(s.type);
              return indexed_transformation::result_type{
                std::pair{std::move(field), std::move(s.array)}};
            },
        };
      } else {
        // TODO: Handle the more general case.
        TENZIR_ASSERT(assignment.left.path.size() == 1);
        TENZIR_ASSERT(slice.columns() > 0);
        transformation = indexed_transformation{
          .index
          = offset{caf::get<record_type>(slice.schema()).num_fields() - 1},
          .fun =
            [&](struct record_type::field field,
                std::shared_ptr<arrow::Array> array) {
              auto result = indexed_transformation::result_type{};
              result.emplace_back(std::move(field), std::move(array));
              result.emplace_back(decltype(field){assignment.left.path[0].name,
                                                  s.type},
                                  std::move(s.array));
              return result;
            },
        };
      }
      // TODO
      if (transformation.index.empty()) {
        auto record = caf::get_if<arrow::StructArray>(&*s.array);
        if (s.type.name().empty()) {
          s.type = type{"tenzir.set", s.type};
        }
        // TODO
        TENZIR_ASSERT(record);
        auto fields = record->Flatten().ValueOrDie();
        result = table_slice{arrow::RecordBatch::Make(s.type.to_arrow_schema(),
                                                      s.length(), fields),
                             s.type};
        TENZIR_ASSERT_EXPENSIVE(to_record_batch(result)->Validate().ok());
      } else {
        result = transform_columns(result, {transformation});
      }
      // TODO!!
    }
    co_yield result;
  }
}

} // namespace tenzir::tql2
