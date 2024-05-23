//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/check_type.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/registry.hpp"

namespace tenzir::tql2 {

namespace {

using namespace ast;

class type_checker {
public:
  using result = std::optional<type>;

  explicit type_checker(session ctx) : ctx_{ctx} {
  }

  auto visit(const literal& x) -> result {
    return x.value.match(
      []<class T>(const T&) -> type {
        return type{data_to_type_t<T>{}};
      },
      [](const null&) -> type {
        return type{null_type{}};
      });
  }

  // auto visit(const path& x) -> result {
  //   if (x.this_ && x.path.empty()) {
  //     return type{record_type{}};
  //   }
  //   return std::nullopt;
  // }

  auto visit(const ast::expression& x) -> result {
    return x.match([&](auto& y) {
      return visit(y);
    });
  }

  auto visit(const binary_expr& x) -> result {
    // TODO: This is just for testing.
    auto left = visit(x.left);
    auto right = visit(x.right);
    if (left && right && left != right) {
      diagnostic::error("cannot {} `{}` and `{}`", x.op.inner, *left, *right)
        .primary(x.op.source)
        .secondary(x.left.get_location(), "{}", *left)
        .secondary(x.right.get_location(), "{}", *right)
        .emit(ctx_.dh());
      return std::nullopt;
    }
    using enum binary_op;
    switch (x.op.inner) {
      case add:
      case sub:
      case mul:
      case div:
        if (left) {
          return left;
        }
        return right;
      case eq:
      case neq:
      case gt:
      case ge:
      case lt:
      case le:
      case and_:
      case or_:
      case in:
        return type{bool_type{}};
    }
    TENZIR_UNREACHABLE();
  }

  auto visit(const unary_expr& x) -> result {
    // TODO
    auto ty_opt = visit(x.expr);
    if (not ty_opt) {
      return {};
    }
    auto ty = std::move(*ty_opt);
    switch (x.op.inner) {
      case unary_op::pos:
      case unary_op::neg:
        if (ty.kind().none_of<int64_type, double_type>()) {
          diagnostic::error("cannot {} `{}`", x.op.inner, ty)
            .primary(x.op.source)
            .secondary(x.expr.get_location(), "{}", ty)
            .emit(ctx_.dh());
          return std::nullopt;
        }
        return ty;
      case unary_op::not_:
        if (ty != type{bool_type{}}) {
          diagnostic::error("cannot {} `{}`", x.op.inner, ty)
            .primary(x.op.source)
            .secondary(x.expr.get_location(), "{}", ty)
            .emit(ctx_.dh());
          return std::nullopt;
        }
        return ty;
    }
    TENZIR_UNREACHABLE();
  }

  auto visit(const function_call& x) -> result {
    auto subject = std::optional<result>{};
    if (x.subject) {
      subject = visit(*x.subject);
    }
    auto args = std::vector<result>{};
    for (auto& arg : x.args) {
      arg.match(
        [&](assignment& x) {
          args.push_back(visit(x.right));
        },
        [&](auto&) {
          args.push_back(visit(arg));
        });
    }
    if (not x.fn.ref.resolved()) {
      return std::nullopt;
    }
    // TODO: Improve.
    return std::nullopt;
    // auto fn
    //   = std::get_if<std::unique_ptr<function_def>>(&ctx_.reg().get(x.fn.ref));
    // TENZIR_ASSERT(fn);
    // TENZIR_ASSERT(*fn);
    // // TODO: This does not respect named arguments.
    // auto info = function_def::check_info{x.fn.get_location(), x.args, args};
    // return (*fn)->check(info, ctx_);
  }

  auto visit(const assignment& x) -> result {
    visit(x.right);
    // TODO
    diagnostic::error("assignments are not allowed here")
      .primary(x.get_location())
      .hint("if you want to compare for equality, use `==` instead")
      .emit(ctx_.dh());
    return std::nullopt;
  }

  auto visit(const pipeline_expr& x) -> result {
    // TODO: How would this work?
    (void)x;
    return std::nullopt;
  }

  auto visit(const ast::record& x) -> result {
    // TODO: Don't we want to propagate fields etc?
    // Or can we perhaps do this with const eval?
    for (auto& y : x.content) {
      y.match(
        [&](const ast::record::field& z) {
          visit(z.expr);
        },
        [&](const ast::record::spread& z) {
          diagnostic::error("not implemented yet")
            .primary(z.expr.get_location())
            .emit(ctx_.dh());
        });
    }
    return type{record_type{}};
  }

  auto visit(const ast::list& x) -> result {
    // TODO: Content type?
    for (auto& y : x.items) {
      visit(y);
    }
    return type{list_type{null_type{}}};
  }

  auto visit(const field_access& x) -> result {
    // TODO: Field types?
    auto ty = visit(x.left);
    if (ty && ty->kind().is_not<record_type>()) {
      diagnostic::error("type `{}` has no fields", *ty)
        .primary(x.name.location)
        .emit(ctx_.dh());
    }
    return std::nullopt;
  }

  auto visit(const meta& x) -> result {
    // TODO
    TENZIR_UNUSED(x);
    return std::nullopt;
  }

  template <class T>
  auto visit(const T& x) -> result {
    TENZIR_UNUSED(x);
    TENZIR_WARN("unimplemented type check for {}", typeid(T).name());
    return std::nullopt;
  }

private:
  session ctx_;
};

} // namespace

auto check_type(const ast::expression& expr, session ctx)
  -> std::optional<type> {
  return type_checker{ctx}.visit(expr);
}

void check_assignment(const ast::assignment& x, session ctx) {
  auto ty = type_checker{ctx}.visit(x.right);
  // if (x.left.this_ && x.left.path.empty()) {
  //   if (ty && *ty != type{record_type{}}) {
  //     diagnostic::error("only records can be assigned to `this`")
  //       .primary(x.right.get_location(), "this is `{}`", *ty)
  //       .emit(ctx.dh());
  //   }
  // }
}

} // namespace tenzir::tql2
