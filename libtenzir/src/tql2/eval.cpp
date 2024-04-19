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
#include "tenzir/try.hpp"

#include <arrow/compute/api_scalar.h>

#include <ranges>

/// TODO:
/// - Reduce series expansion. For example, `src_ip in [1.2.3.4, 1.2.3.5]`
///   currently creates `length` copies of the list.
/// - Optimize expressions, e.g., constant folding, compute offsets.
/// - Short circuiting, active rows.
/// - Stricter behavior for const-eval, or same behavior? For example, overflow.
/// - Modes for "must be constant", "prefer constant", "prefer runtime", "must
///   be runtime".
/// - Integrate type checker?

namespace tenzir::tql2 {

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

void ensure(const arrow::Status& status) {
  TENZIR_ASSERT(status.ok(), status.ToString());
}

template <class T>
[[nodiscard]] auto ensure(arrow::Result<T> result) -> T {
  ensure(result.status());
  return result.MoveValueUnsafe();
}

template <std::derived_from<arrow::ArrayBuilder> T>
[[nodiscard]] auto finish(T& x) {
  using Type = std::conditional_t<std::same_as<arrow::StringBuilder, T>,
                                  arrow::StringType, typename T::TypeClass>;
  auto result = std::shared_ptr<typename arrow::TypeTraits<Type>::ArrayType>{};
  ensure(x.Finish(&result));
  TENZIR_ASSERT(result);
  return result;
}

auto data_to_series(const data& x, int64_t length) -> series {
  // TODO: This is overkill.
  auto b = series_builder{};
  for (auto i = int64_t{0}; i < length; ++i) {
    b.data(x);
  }
  return b.finish_assert_one_array();
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
    -> std::shared_ptr<arrow::StringArray> {
    auto b = arrow::StringBuilder{};
    ensure(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (l.IsNull(i) || r.IsNull(i)) {
        b.UnsafeAppendNull();
        continue;
      }
      auto lv = l.GetView(i);
      auto rv = r.GetView(i);
      ensure(b.Append(lv));
      ensure(b.ExtendCurrent(rv));
    }
    return finish(b);
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
template <numeric_type T>
struct EvalBinOp<ast::binary_op::eq, T, T> {
  static auto
  eval(const type_to_arrow_array_t<T>& l, const type_to_arrow_array_t<T>& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    ensure(b.Reserve(l.length()));
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
    return finish(b);
  }
};

template <numeric_type T>
struct EvalBinOp<ast::binary_op::gt, T, T> {
  static auto
  eval(const type_to_arrow_array_t<T>& l, const type_to_arrow_array_t<T>& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    ensure(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      if (ln || rn) {
        b.UnsafeAppendNull();
      } else {
        auto lv = l.Value(i);
        auto rv = r.Value(i);
        b.UnsafeAppend(lv > rv);
      }
    }
    return finish(b);
  }
};

// TODO: We probably don't want this.
template <>
struct EvalBinOp<ast::binary_op::and_, bool_type, bool_type> {
  static auto eval(const arrow::BooleanArray& l, const arrow::BooleanArray& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: Bad.
    auto has_null = l.null_bitmap() || r.null_bitmap();
    auto has_offset = l.offset() != 0 || r.offset() != 0;
    if (not has_null && not has_offset) {
      auto buffer = ensure(arrow::AllocateBitmap(l.length()));
      auto size = (l.length() + 7) / 8;
      TENZIR_ASSERT(l.values()->size() >= size);
      TENZIR_ASSERT(r.values()->size() >= size);
      auto l_ptr = l.values()->data();
      auto r_ptr = r.values()->data();
      auto o_ptr = buffer->mutable_data();
      for (auto i = int64_t{0}; i < size; ++i) {
        o_ptr[i] = l_ptr[i] & r_ptr[i];
      }
      return std::make_shared<arrow::BooleanArray>(l.length(),
                                                   std::move(buffer));
    }
    auto b = arrow::BooleanBuilder{};
    ensure(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto lv = l.Value(i);
      auto rv = r.Value(i);
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      auto lt = not ln && lv;
      auto lf = not ln && not lv;
      auto rt = not rn && rv;
      auto rf = not rn && not rv;
      if (lt && rt) {
        b.UnsafeAppend(true);
      } else if (lf || rf) {
        b.UnsafeAppend(false);
      } else {
        b.UnsafeAppendNull();
      }
    }
    return finish(b);
  }
};

template <ast::binary_op Op, concrete_type L>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, L, null_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::NullArray& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    constexpr auto invert = Op == ast::binary_op::neq;
    // TODO: This is bad.
    TENZIR_UNUSED(r);
    auto buffer = ensure(arrow::AllocateBitmap(l.length()));
    auto& null_bitmap = l.null_bitmap();
    if (not null_bitmap) {
      // All non-null, except if `null_type`.
      auto value = (std::same_as<L, null_type> != invert) ? 0xFF : 0x00;
      std::memset(buffer->mutable_data(), value, buffer->size());
    } else {
      TENZIR_ASSERT(buffer->size() <= null_bitmap->size());
      auto buffer_ptr = buffer->mutable_data();
      auto null_ptr = null_bitmap->data();
      auto length = detail::narrow<size_t>(buffer->size());
      for (auto i = size_t{0}; i < length; ++i) {
        // TODO
        buffer_ptr[i] = invert ? null_ptr[i] : ~null_ptr[i];
      }
    }
    return std::make_shared<arrow::BooleanArray>(l.length(), std::move(buffer));
  }
};

template <ast::binary_op Op, concrete_type R>
  requires((Op == ast::binary_op::eq || Op == ast::binary_op::neq)
           && not std::same_as<R, null_type>)
struct EvalBinOp<Op, null_type, R> {
  static auto eval(const arrow::NullArray& l, const type_to_arrow_array_t<R>& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    return EvalBinOp<Op, R, null_type>::eval(r, l);
  }
};

template <ast::binary_op Op>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: This is bad.
    constexpr auto invert = Op == ast::binary_op::neq;
    auto b = arrow::BooleanBuilder{};
    ensure(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      auto equal = bool{};
      if (ln != rn) {
        equal = false;
      } else if (ln && rn) {
        equal = true;
      } else {
        equal = l.Value(i) == r.Value(i);
      }
      b.UnsafeAppend(equal != invert);
    }
    return finish(b);
  }
};

template <ast::unary_op Op, concrete_type T>
struct EvalUnOp;

template <>
struct EvalUnOp<ast::unary_op::not_, bool_type> {
  static auto eval(const arrow::BooleanArray& x)
    -> std::shared_ptr<arrow::BooleanArray> {
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
  explicit evaluator(const table_slice* input, diagnostic_handler& dh)
    : input_{input},
      length_{input ? detail::narrow<int64_t>(input->rows()) : 1},
      dh_{dh} {
  }

  // auto to_series(const value& val) -> series {
  //   return value_to_series(val, length());
  // }

  auto eval(const ast::expression& x) -> series {
    return x.match([&](auto& y) {
      return eval(y);
    });
  }

  auto eval(const ast::literal& x) -> series {
    return data_to_series(x.as_data(), length_);
  }

  auto null() -> series {
    return data_to_series(caf::none, length_);
  }

  auto eval(const ast::record& x) -> series {
    // TODO: Soooo bad.
    auto fields = detail::stable_map<std::string, series>{};
    for (auto& item : x.content) {
      item.match(
        [&](const ast::record::field& field) {
          auto val = eval(field.expr);
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
      length_, nullptr, std::vector(field_names.begin(), field_names.end()),
      std::vector(field_arrays.begin(), field_arrays.end()));
    return series{
      type{record_type{std::vector(field_types.begin(), field_types.end())}},
      std::move(result),
    };
  }

  auto eval(const ast::list& x) -> series {
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
      auto array = eval(item);
      if (not arrays.empty()) {
        // TODO:
        TENZIR_ASSERT(array.type == arrays[0].type);
      }
      arrays.push_back(std::move(array));
    }
    // arrays = [<1, 3>, <2, 4>]
    // TODO:
    if (arrays.empty()) {
      auto b = series_builder{type{list_type{null_type{}}}};
      for (auto i = int64_t{0}; i < length_; ++i) {
        b.list();
      }
      return b.finish_assert_one_array();
    }
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

  auto eval(const ast::selector& x) -> series {
    if (not input_) {
      diagnostic::error("expected a constant expression")
        .primary(x.get_location())
        .emit(dh_);
      // TODO: This is pretty bad.
      throw std::monostate{};
    }
    auto result = resolve(x, *input_);
    return result.match(
      [](series& x) -> series {
        return std::move(x);
      },
      [&](resolve_error& err) -> series {
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
        return null();
      });
  }

  auto eval(const ast::function_call& x) -> series {
    TENZIR_ASSERT(x.fn.ref.resolved());
    auto segments = x.fn.ref.segments();
    TENZIR_ASSERT(segments.size() == 1);
    auto fn = plugins::find<function_plugin>("tql2." + segments[0]);
    TENZIR_ASSERT(fn);
    auto args = std::vector<series>{};
    for (auto& arg : x.args) {
      args.push_back(eval(arg));
    }
    auto ret = fn->eval(x, length_, std::move(args), dh_);
    TENZIR_ASSERT(ret.length() == length_);
    return ret;
  }

  template <ast::unary_op Op>
  auto eval(const ast::unary_expr& x, const series& v) -> series {
    // auto v = to_series(eval(x.expr));
    TENZIR_ASSERT(x.op.inner == Op);
    return caf::visit(
      [&]<concrete_type T>(const T&) -> series {
        if constexpr (caf::detail::is_complete<EvalUnOp<Op, T>>) {
          auto& a = caf::get<type_to_arrow_array_t<T>>(*v.array);
          auto oa = EvalUnOp<Op, T>::eval(a);
          auto ot = type::from_arrow(*oa->type());
          return series{std::move(ot), std::move(oa)};
        } else {
          diagnostic::warning("unary operator `{}` not implemented for `{}`",
                              x.op.inner, v.type.kind())
            .primary(x.get_location())
            .emit(dh_);
          return null();
        }
      },
      v.type);
  }

  auto eval(const ast::unary_expr& x) -> series {
    auto v = eval(x.expr);
    using enum ast::unary_op;
    switch (x.op.inner) {
#define X(op)                                                                  \
  case op:                                                                     \
    return eval<op>(x, v)
      X(pos);
      X(neg);
      X(not_);
#undef X
    }
    TENZIR_UNREACHABLE();
  }

  template <ast::binary_op Op>
  auto eval(const ast::binary_expr& x, const series& l, const series& r)
    -> series {
    TENZIR_ASSERT(x.op.inner == Op);
    TENZIR_ASSERT(l.length() == r.length());
    return caf::visit(
      [&]<concrete_type L, concrete_type R>(const L&, const R&) -> series {
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
            .primary(x.get_location())
            .emit(dh_);
          return null();
        }
      },
      l.type, r.type);
  }

  auto eval(const ast::binary_expr& x) -> series {
    using enum ast::binary_op;
    switch (x.op.inner) {
#define X(op)                                                                  \
  case op:                                                                     \
    return eval<op>(x, eval(x.left), eval(x.right))
      X(add);
      X(sub);
      X(mul);
      X(div);
      X(eq);
      X(neq);
      X(gt);
      X(ge);
      X(lt);
      X(le);
      X(in);
#undef X
      case and_: {
        // TODO: How does short circuiting work?
        // 1) Evaluate left.
        auto l = eval(x.left);
        // 2) Evaluate right, but discard diagnostics if false.
        // TODO
        auto r = eval(x.right);
        return eval<and_>(x, l, r);
      }
      case or_:
        TENZIR_TODO();
    }
    TENZIR_UNREACHABLE();
  }

  auto eval(const ast::field_access& x) -> series {
    auto l = eval(x.left);
    auto rec_ty = caf::get_if<record_type>(&l.type);
    if (not rec_ty) {
      diagnostic::warning("cannot access field of non-record type")
        .primary(x.dot.combine(x.name.location))
        .secondary(x.left.get_location(), "type `{}`", l.type.kind())
        .emit(dh_);
      return null();
    }
    auto& s = caf::get<arrow::StructArray>(*l.array);
    for (auto [i, field] : detail::enumerate<int>(rec_ty->fields())) {
      if (field.name == x.name.name) {
        return series{field.type, s.field(i)};
      }
    }
    diagnostic::warning("record does not have this field")
      .primary(x.name.location)
      .emit(dh_);
    return null();
  }

  auto eval(const ast::assignment& x) -> series {
    diagnostic::warning("unexpected assignment")
      .primary(x.get_location())
      .emit(dh_);
    return null();
  }

  auto eval(const auto& x) -> series {
    return not_implemented(x);
  }

  auto not_implemented(const auto& x) -> series {
    diagnostic::warning("eval not implemented yet for: {:?}",
                        use_default_formatter(x))
      .primary(x.get_location())
      .emit(dh_);
    return null();
  }

private:
  const table_slice* input_;
  int64_t length_;
  diagnostic_handler& dh_;
};

} // namespace

auto eval(const ast::expression& expr, const table_slice& input,
          diagnostic_handler& dh) -> series {
  return evaluator{&input, dh}.eval(expr);
}

auto const_eval(const ast::expression& expr, context& ctx)
  -> std::optional<data> {
  try {
    auto result = evaluator{nullptr, ctx.dh()}.eval(expr);
    TENZIR_ASSERT(result.length() == 1);
    return materialize(value_at(result.type, *result.array, 0));
  } catch (std::monostate) {
    return std::nullopt;
  }
}

} // namespace tenzir::tql2
