//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"
#include "tenzir/tql2/eval_impl2.hpp"
#include "tenzir2/type_system/array/access.hpp"
#include "tenzir2/type_system/array/builder.hpp"

#include <concepts>
#include <limits>
#include <string_view>
#include <type_traits>

namespace tenzir2 {

namespace {

template <tenzir::ast::unary_op Op, data_type T>
struct EvalUnOp {};

template <data_type T>
constexpr auto data_type_name() -> std::string_view {
  if constexpr (std::same_as<T, null>) {
    return "null";
  } else if constexpr (std::same_as<T, bool>) {
    return "bool";
  } else if constexpr (std::same_as<T, std::int64_t>) {
    return "int64";
  } else if constexpr (std::same_as<T, std::uint64_t>) {
    return "uint64";
  } else if constexpr (std::same_as<T, double>) {
    return "double";
  } else if constexpr (std::same_as<T, std::string>) {
    return "string";
  } else if constexpr (std::same_as<T, ip>) {
    return "ip";
  } else if constexpr (std::same_as<T, subnet>) {
    return "subnet";
  } else if constexpr (std::same_as<T, time>) {
    return "time";
  } else if constexpr (std::same_as<T, duration>) {
    return "duration";
  } else if constexpr (std::same_as<T, list>) {
    return "list";
  } else if constexpr (std::same_as<T, record>) {
    return "record";
  } else {
    return "<?>";
  }
}

auto make_null_array(std::ptrdiff_t length, memory::element_state state
                                            = memory::element_state::null)
  -> array_<data> {
  auto builder = array_builder_<data>{};
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    builder.null(state);
  }
  return builder.finish();
}

auto has_active_rows(tenzir::ActiveRows const& active, std::ptrdiff_t length)
  -> bool {
  if (auto constant = active.as_constant()) {
    return *constant;
  }
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    if (active.is_active(static_cast<int64_t>(row))) {
      return true;
    }
  }
  return false;
}

struct null_warn {
  auto operator()(std::string_view) const -> void {
  }
};

template <tenzir::ast::unary_op Op, class T>
inline constexpr auto has_eval_un_op_v
  = requires(array_<T> arr, tenzir::ActiveRows const& active) {
      {
        EvalUnOp<Op, T>::eval(std::move(arr), null_warn{}, active)
      } -> std::same_as<array_<data>>;
    };

template <class Array>
struct array_value_type;

template <data_type T>
struct array_value_type<array_<T>> {
  using type = T;
};

template <class Array>
using array_value_type_t =
  typename array_value_type<std::remove_cvref_t<Array>>::type;

template <tenzir::ast::unary_op Op>
struct EvalUnOp<Op, null> {
  static auto eval(array_<null> x, auto warn, tenzir::ActiveRows const& active)
    -> array_<data> {
    TENZIR_UNUSED(warn, active);
    return array_<data>{std::move(x)};
  }
};

template <>
struct EvalUnOp<tenzir::ast::unary_op::not_, bool> {
  static auto eval(array_<bool> x, auto warn, tenzir::ActiveRows const& active)
    -> array_<data> {
    TENZIR_UNUSED(warn);
    auto builder = array_builder_<bool>{memory::default_resource()};
    for (auto row = std::ptrdiff_t{0}; row < x.length(); ++row) {
      if (not active.is_active(static_cast<int64_t>(row))
          or x.state(row) != memory::element_state::valid) {
        builder.null();
        continue;
      }
      builder.data(not *x.get(row));
    }
    return builder.finish();
  }
};

template <>
struct EvalUnOp<tenzir::ast::unary_op::neg, std::int64_t> {
  static auto eval(array_<std::int64_t> x, auto warn,
                   tenzir::ActiveRows const& active) -> array_<data> {
    auto builder = array_builder_<std::int64_t>{memory::default_resource()};
    auto overflow = false;
    for (auto row = std::ptrdiff_t{0}; row < x.length(); ++row) {
      if (not active.is_active(static_cast<int64_t>(row))
          or x.state(row) != memory::element_state::valid) {
        builder.null();
        continue;
      }
      auto value = *x.get(row);
      if (value == std::numeric_limits<std::int64_t>::min()) {
        overflow = true;
        builder.null();
        continue;
      }
      builder.data(-value);
    }
    if (overflow) {
      warn("integer overflow");
    }
    return builder.finish();
  }
};

template <>
struct EvalUnOp<tenzir::ast::unary_op::neg, std::uint64_t> {
  static auto eval(array_<std::uint64_t> x, auto warn,
                   tenzir::ActiveRows const& active) -> array_<data> {
    auto builder = array_builder_<std::int64_t>{memory::default_resource()};
    auto overflow = false;
    for (auto row = std::ptrdiff_t{0}; row < x.length(); ++row) {
      if (not active.is_active(static_cast<int64_t>(row))
          or x.state(row) != memory::element_state::valid) {
        builder.null();
        continue;
      }
      auto value = *x.get(row);
      if (value
          > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())
              + 1) {
        overflow = true;
        builder.null();
        continue;
      }
      builder.data(-static_cast<std::int64_t>(value));
    }
    if (overflow) {
      warn("integer overflow");
    }
    return builder.finish();
  }
};

template <>
struct EvalUnOp<tenzir::ast::unary_op::neg, double> {
  static auto eval(array_<double> x, auto warn,
                   tenzir::ActiveRows const& active) -> array_<data> {
    TENZIR_UNUSED(warn);
    auto builder = array_builder_<double>{memory::default_resource()};
    for (auto row = std::ptrdiff_t{0}; row < x.length(); ++row) {
      if (not active.is_active(static_cast<int64_t>(row))
          or x.state(row) != memory::element_state::valid) {
        builder.null();
        continue;
      }
      builder.data(-*x.get(row));
    }
    return builder.finish();
  }
};

template <>
struct EvalUnOp<tenzir::ast::unary_op::neg, duration> {
  static auto eval(array_<duration> x, auto warn,
                   tenzir::ActiveRows const& active) -> array_<data> {
    auto builder = array_builder_<duration>{memory::default_resource()};
    auto overflow = false;
    for (auto row = std::ptrdiff_t{0}; row < x.length(); ++row) {
      if (not active.is_active(static_cast<int64_t>(row))
          or x.state(row) != memory::element_state::valid) {
        builder.null();
        continue;
      }
      auto value = *x.get(row);
      if (value.count() == std::numeric_limits<duration::rep>::min()) {
        overflow = true;
        builder.null();
        continue;
      }
      builder.data(duration{-value});
    }
    if (overflow) {
      warn("duration negation overflow");
    }
    return builder.finish();
  }
};

template <tenzir::ast::unary_op Op>
auto eval_un_op(evaluator& self, tenzir::ast::unary_expr const& x,
                tenzir::ActiveRows const& active) -> array_<data> {
  auto operand = self.eval(x.expr, active);
  auto active_rows = has_active_rows(active, self.length());
  return access::transform(std::move(operand), [&](auto&& arr) -> array_<data> {
    using type = array_value_type_t<decltype(arr)>;
    if constexpr (has_eval_un_op_v<Op, type>) {
      return EvalUnOp<Op, type>::eval(
        std::move(arr),
        [&](std::string_view msg) {
          tenzir::diagnostic::warning("{}", msg).primary(x).emit(self.ctx());
        },
        active);
    } else {
      if (active_rows) {
        tenzir::diagnostic::warning("unary operator `{}` not implemented for "
                                    "`{}`",
                                    x.op.inner, data_type_name<type>())
          .primary(x)
          .emit(self.ctx());
      }
      return make_null_array(arr.length());
    }
  });
}

} // namespace

auto evaluator::eval(tenzir::ast::unary_expr const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  using enum tenzir::ast::unary_op;
  switch (x.op.inner) {
    case pos:
      return eval(x.expr, active);
    case move:
      if (tenzir::ast::field_path::try_from(x.expr)) {
        tenzir::diagnostic::warning("move is not supported here")
          .primary(x.op, "has no effect")
          .hint("move only works on fields within assignments")
          .emit(ctx_);
      } else {
        tenzir::diagnostic::warning("move has no effect")
          .primary(x.expr, "is not a field")
          .hint("move only works on fields within assignments")
          .emit(ctx_);
      }
      return eval(x.expr, active);
    case not_:
      return eval_un_op<not_>(*this, x, active);
    case neg:
      return eval_un_op<neg>(*this, x, active);
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir2
