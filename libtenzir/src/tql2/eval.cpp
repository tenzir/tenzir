//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/context.hpp"

#include <arrow/compute/api_scalar.h>

#include <ranges>

namespace tenzir::tql2 {

namespace {

class const_evaluator {
public:
  explicit const_evaluator(context& ctx) : ctx_{ctx} {
  }

  auto eval(const ast::expression& x) -> data {
    return x.match([&](auto& y) {
      return eval(y);
    });
  }

  auto eval(const ast::record& x) -> data {
    auto result = record{};
    for (auto& y : x.content) {
      y.match(
        [&](const ast::record::field& y) {
          auto val = eval(y.expr);
          auto inserted = result.emplace(y.name.name, val).second;
          if (not inserted) {
            diagnostic::error("field `{}` already exists", y.name.name)
              .primary(y.name.location)
              .throw_();
          }
        },
        [](const auto&) {
          // TODO
          diagnostic::error("not implemented").throw_();
        });
    }
    return result;
  }

  auto eval(const ast::list& x) -> data {
    auto result = list{};
    for (auto& y : x.items) {
      result.push_back(eval(y));
    }
    return result;
  }

  auto eval(const ast::selector& x) -> data {
    diagnostic::error("expected a constant expression")
      .primary(x.get_location())
      .throw_();
  }

  auto eval(const ast::literal& x) -> data {
    return x.value.match(
      [&](const auto& y) -> data {
        return y;
      },
      [&](ast::null) -> data {
        return caf::none;
      });
  }

  auto eval(const ast::unary_expr& x) -> data {
    // TODO
    auto val = eval(x.expr);
    auto not_implemented = [&] {
      diagnostic::error("unary op eval not implemented")
        .primary(x.get_location())
        .throw_();
    };
    return caf::visit(
      [&]<class T>(const T& y) -> data {
        if constexpr (std::signed_integral<T> || std::floating_point<T>) {
          if (x.op.inner != ast::unary_op::neg) {
            not_implemented();
          }
          return -y;
        }
        not_implemented();
        TENZIR_UNREACHABLE();
      },
      val);
  }

  auto eval(const ast::binary_expr& x) -> data {
    auto left = eval(x.left);
    auto right = eval(x.right);
    auto not_implemented = [&] {
      diagnostic::error("binary op eval not implemented")
        .primary(x.get_location())
        .throw_();
    };
    return caf::visit(
      [&]<class L, class R>(const L& l, const R& r) -> data {
        if constexpr (std::same_as<L, R>) {
          using T = L;
          if constexpr (std::signed_integral<T> || std::floating_point<T>) {
            switch (x.op.inner) {
              case ast::binary_op::add:
                if (l > 0 && r > std::numeric_limits<T>::max() - l) {
                  diagnostic::error("integer overflow")
                    .primary(x.get_location())
                    .throw_();
                }
                if (l < 0 && r < std::numeric_limits<T>::min() - l) {
                  diagnostic::error("integer underflow")
                    .primary(x.get_location())
                    .throw_();
                }
                return l + r;
              case ast::binary_op::sub:
                return l - r;
              default:
                break;
            }
          }
        }
        not_implemented();
        TENZIR_UNREACHABLE();
      },
      left, right);
  }

  auto eval(const ast::function_call& x) -> data {
    if (not x.fn.ref.resolved()) {
      throw std::monostate{};
    }
#if 1
    diagnostic::error("not implemented eval for function").throw_();
#else
    auto& entity = ctx_.reg().get(x.fn.ref);
    auto fn = std::get_if<std::unique_ptr<function_def>>(&entity);
    // TODO
    TENZIR_ASSERT(fn);
    TENZIR_ASSERT(*fn);
    auto args = std::vector<located<data>>{};
    args.reserve(x.args.size());
    for (auto& arg : x.args) {
      auto val = eval(arg);
      args.emplace_back(val, arg.get_location());
    }
    auto result = (*fn)->evaluate(x.fn.get_location(), std::move(args), ctx_);
    if (not result) {
      throw std::monostate{};
    }
    return std::move(*result);
#endif
  }

  auto eval(const ast::dollar_var& x) -> data {
    diagnostic::error("TODO: eval dollar").primary(x.get_location()).throw_();
  }

  auto eval(const ast::entity& x) -> data {
    // we know this must be a constant
    TENZIR_UNUSED(x);
    return caf::none;
  }

  template <class T>
  auto eval(const T& x) -> data {
    diagnostic::error("eval not implemented").primary(x.get_location()).throw_();
  }

private:
  [[maybe_unused]] context& ctx_;
};

} // namespace

auto const_eval(const ast::expression& expr, context& ctx)
  -> std::optional<data> {
  try {
    return const_evaluator{ctx}.eval(expr);
  } catch (diagnostic& d) {
    ctx.dh().emit(std::move(d));
    // TODO
    return std::nullopt;
  } catch (std::monostate) {
    return std::nullopt;
  }
}

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

// TODO: Generalize.
template <>
struct EvalBinOp<ast::binary_op::eq, int64_type, int64_type> {
  static auto eval(const arrow::Int64Array& l, const arrow::Int64Array& r)
    -> std::shared_ptr<arrow::Array> {
    auto b = arrow::BooleanBuilder{};
    (void)b.Reserve(l.length());
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      if (ln && rn) {
        b.UnsafeAppend(true);
      } else if (ln != rn) {
        b.UnsafeAppend(false);
      } else {
        auto lv = l.Value(i);
        auto rv = r.Value(i);
        b.UnsafeAppend(lv == rv);
      }
    }
    return b.Finish().ValueOrDie();
  }
};

void ensure(const arrow::Status& status) {
  TENZIR_ASSERT(status.ok(), status.ToString());
}

template <class T>
auto ensure(arrow::Result<T> result) -> T {
  ensure(result.status());
  return result.MoveValueUnsafe();
}

template <concrete_type L>
struct EvalBinOp<ast::binary_op::eq, L, null_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::NullArray& r)
    -> std::shared_ptr<arrow::Array> {
    // TODO: This is bad.
    TENZIR_UNUSED(r);
    auto buffer = ensure(arrow::AllocateBitmap(l.length()));
    auto& null_bitmap = l.null_bitmap();
    if (not null_bitmap) {
      // All non-null, except if `null_type`.
      auto value = std::same_as<L, null_type> ? 0xFF : 0x00;
      std::memset(buffer->mutable_data(), value, buffer->size());
    } else {
      TENZIR_ASSERT(buffer->size() == null_bitmap->size());
      auto buffer_ptr = buffer->mutable_data();
      auto null_ptr = null_bitmap->data();
      auto length = detail::narrow<size_t>(buffer->size());
      for (auto i = size_t{0}; i < length; ++i) {
        // TODO
        buffer_ptr[i] = ~null_ptr[i];
      }
    }
    return std::make_shared<arrow::BooleanArray>(l.length(), std::move(buffer));
  }
};

template <ast::unary_op Op, concrete_type T>
struct EvalUnOp;

template <>
struct EvalUnOp<ast::unary_op::not_, bool_type> {
  static auto eval(const arrow::BooleanArray& x)
    -> std::shared_ptr<arrow::Array> {
    // TODO
    auto input = x.values();
    auto output = ensure(arrow::AllocateBuffer(input->size()));
    auto input_ptr = input->data();
    auto output_ptr = output->mutable_data();
    auto length = detail::narrow<size_t>(input->size());
    for (auto i = size_t{0}; i < length; ++i) {
      output_ptr[i] = ~input_ptr[i]; // NOLINT
    }
    return std::make_shared<arrow::BooleanArray>(x.length(), std::move(output),
                                                 x.null_bitmap(),
                                                 x.data()->null_count,
                                                 x.offset());
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
          // TODO: Not possible?
          // TODO: Where coercion? => coercion is done in kernel.
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

  template <ast::unary_op Op>
  auto eval(const ast::unary_expr& x, const series& v) -> value {
    // auto v = to_series(eval(x.expr));
    TENZIR_ASSERT(x.op.inner == Op);
    return caf::visit(
      [&]<concrete_type T>(const T&) -> value {
        if constexpr (caf::detail::is_complete<EvalUnOp<Op, T>>) {
          auto& a = caf::get<type_to_arrow_array_t<T>>(*v.array);
          auto oa = EvalUnOp<Op, T>::eval(a);
          auto ot = type::from_arrow(*oa->type());
          return series{std::move(ot), std::move(oa)};
        } else {
          return caf::none;
        }
      },
      v.type);
  }

  auto eval(const ast::unary_expr& x) -> value {
    auto v = to_series(eval(x.expr));
    switch (x.op.inner) {
      case ast::unary_op::not_:
        return eval<ast::unary_op::not_>(x, v);
      default:
        return caf::none;
    }
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
      case ast::binary_op::eq:
        return eval<ast::binary_op::eq>(x, l, r);
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

  auto eval(const ast::field_access& x) -> value {
    auto l = to_series(eval(x.left));
    auto rec_ty = caf::get_if<record_type>(&l.type);
    if (not rec_ty) {
      diagnostic::error("cannot access field of non-record type")
        .primary(x.dot.combine(x.name.location))
        .secondary(x.left.get_location(), "type `{}`", l.type.kind())
        .emit(dh_);
      return caf::none;
    }
    auto& s = caf::get<arrow::StructArray>(*l.array);
    for (auto [i, field] : detail::enumerate<int>(rec_ty->fields())) {
      if (field.name == x.name.name) {
        return series{field.type, s.field(i)};
      }
    }
    diagnostic::error("record does not have this field")
      .primary(x.name.location)
      .emit(dh_);
    return caf::none;
  }

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
};

} // namespace

auto eval(const ast::expression& expr, const table_slice& input,
          diagnostic_handler& dh) -> series {
  return value_to_series(evaluator{input, dh}.eval(expr), input.rows());
}

} // namespace tenzir::tql2
