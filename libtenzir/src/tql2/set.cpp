//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/set.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/detail/operators.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"

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

auto value_to_series(value& val, int64_t length) -> series {
  return val.match(
    [&](data& x) {
      // TODO: This is overkill.
      auto b = series_builder{};
      for (auto i = int64_t{0}; i < length; ++i) {
        b.data(x);
      }
      return b.finish_assert_one_array();
    },
    [&](series& x) {
      TENZIR_ASSERT(x.length() == length);
      return x;
    });
}

// TODO: not good.
template <class T>
concept Addable = requires(T x, T y) {
  { x + y } -> std::same_as<T>;
};

class evaluator {
public:
  explicit evaluator(const table_slice& input, diagnostic_handler& dh)
    : input_{input}, dh_{dh} {
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
    auto result = record{};
    for (auto& item : x.content) {
      item.match(
        [&](const ast::record::field& field) {
          auto val = eval(field.expr);
          auto cval = std::get_if<data>(&val);
          if (not cval) {
            TENZIR_TODO();
          }
          auto [_, inserted]
            = result.emplace(field.name.name, std::move(*cval));
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
    return result;
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
    return not_implemented(x);
  }

  auto eval(const ast::binary_expr& x) -> value {
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
      // We are using `slice` here, not `result`!
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
          .index = offset{slice.columns() - 1},
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
      result = transform_columns(result, {transformation});
      // TODO!!
    }
    co_yield result;
  }
}

} // namespace tenzir::tql2
