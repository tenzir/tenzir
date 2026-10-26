//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/const_eval.hpp"

#include "tenzir/async.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/stringify.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/try.hpp"

#include <concepts>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "diagnostics.hpp"

namespace tenzir::nova {

namespace {

/// Analyzes a binding that may look up secrets in a single post-order pass, and
/// folds every maximal subtree that does not require startup into a constant,
/// so that only the secret lookups and the operations applied to them remain.
/// Lambda bodies run once per element, so they are never folded.
class StartupBindingFolder : public ast::visitor<StartupBindingFolder> {
public:
  explicit StartupBindingFolder(base_ctx ctx) : ctx_{ctx} {
  }

  auto visit(ast::expression& expr) -> void {
    if (failed) {
      return;
    }
    auto outer = std::exchange(frame_, Frame{});
    enter(expr);
    auto inner = std::exchange(frame_, std::move(outer));
    if (inner.requires_startup) {
      // Folded children are evaluated once, so their determinism is moot.
      for (auto& child : inner.foldable) {
        auto value = tenzir::const_eval(*child.expr, ctx_);
        if (not value) {
          failed = true;
          return;
        }
        *child.expr = ast::constant::make(std::move(*value));
      }
    } else {
      for (auto const& child : inner.foldable) {
        inner.deterministic &= child.deterministic;
      }
    }
    frame_.requires_startup |= inner.requires_startup;
    auto foldable = expr.match([]<class T>(T const&) {
      return _::invalid_value_kind<T>().empty()
             and not std::same_as<T, ast::constant>;
    });
    if (lambda_depth_ == 0 and foldable and not inner.requires_startup) {
      frame_.foldable.push_back({&expr, inner.deterministic});
    } else {
      frame_.deterministic &= inner.deterministic;
    }
  }

  /// Secret lookups are stable within the invocation that resolves them.
  auto visit(ast::function_call& call) -> void {
    auto const& ref = call.fn.ref;
    if (ref.pkg() == entity_pkg_std and ref.segments().size() == 1
        and ref.segments().front() == "secret") {
      frame_.requires_startup = true;
    } else {
      frame_.deterministic &= ctx_.reg().get(call).is_deterministic();
    }
    enter(call);
  }

  /// An unbound pipeline `let` has no value yet.
  auto visit(ast::dollar_var&) -> void {
    frame_.deterministic = false;
  }

  /// A subpipeline is not a value.
  auto visit(ast::pipeline_expr&) -> void {
    frame_.deterministic = false;
  }

  auto visit(ast::lambda_expr& x) -> void {
    ++lambda_depth_;
    enter(x);
    --lambda_depth_;
  }

  /// The left side of a named argument is its name, not a value.
  auto visit(ast::assignment& x) -> void {
    visit(x.right);
  }

  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto result() const -> StartupBinding {
    auto deterministic = frame_.deterministic;
    for (auto const& child : frame_.foldable) {
      deterministic &= child.deterministic;
    }
    return {frame_.requires_startup, deterministic};
  }

  bool failed = false;

private:
  struct Foldable {
    ast::expression* expr;
    bool deterministic;
  };

  struct Frame {
    bool requires_startup = false;
    bool deterministic = true;
    std::vector<Foldable> foldable;
  };

  base_ctx ctx_;
  size_t lambda_depth_ = 0;
  Frame frame_;
};

/// Registers secrets that Nova functions derived from resolved secrets, such as
/// `secret("key").decode_hex()`, which the resolution itself cannot censor.
auto register_secrets(Data const& value, OpCtx& ctx) -> void {
  match(
    value,
    [&](Secret const& secret) {
      ctx.register_secret(secret);
    },
    [&](Record const& record) {
      for (auto const& [_, field] : record) {
        register_secrets(field, ctx);
      }
    },
    [&](List const& list) {
      for (auto const& element : list) {
        register_secrets(element, ctx);
      }
    },
    [](auto const&) {});
}

} // namespace

auto fold_startup_binding(ast::expression& expr, base_ctx ctx)
  -> failure_or<StartupBinding> {
  auto folder = StartupBindingFolder{ctx};
  folder.visit(expr);
  if (folder.failed) {
    return failure::promise();
  }
  return folder.result();
}

auto const_eval_array(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Array<Data>> {
  auto diagnostics = _::DiagnosticScope{ctx};
  TRY(auto evaluator,
      Evaluator::make(expr, InstantiateCtx{diagnostics.handler(), ctx}));
  auto result = evaluator.eval(EvalCtx{diagnostics.handler()});
  // Errors that did not abort the evaluation (e.g. a function that is not
  // implemented for nova) still make the value meaningless.
  if (diagnostics.failed()) {
    return failure::promise();
  }
  TENZIR_ASSERT(result.length() == 1);
  return result;
}

auto const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Data> {
  TRY(auto result, const_eval_array(expr, ctx));
  // The single row is always requested, so it always holds a value;
  // `materialize_data` preserves an explicit `Null` as Nova `Null`.
  auto value = materialize_data(result.get(0));
  return value;
}

auto const_eval(const ast::expression& expr, OpCtx& ctx)
  -> Task<failure_or<Data>> {
  auto diagnostics = _::DiagnosticScope{ctx.dh()};
  CO_TRY(auto evaluator,
         co_await Evaluator::make(expr, ctx, diagnostics.handler()));
  auto result = evaluator.eval(EvalCtx{diagnostics.handler()});
  if (diagnostics.failed()) {
    co_return failure::promise();
  }
  TENZIR_ASSERT(result.length() == 1);
  auto value = materialize_data(result.get(0));
  register_secrets(value, ctx);
  co_return value;
}

auto try_const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> Option<Data> {
  const registry& reg = ctx;
  if (not expr.is_deterministic(reg)) {
    return None{};
  }
  auto collector = collecting_diagnostic_handler{};
  auto result = const_eval(expr, InstantiateCtx{collector, reg});
  if (not result) {
    return None{};
  }
  std::move(collector).forward_to(ctx);
  return std::move(*result);
}

} // namespace tenzir::nova
