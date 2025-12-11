//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/exec.hpp"
#include "tenzir/finalize_ctx.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/user_defined_operator.hpp"

#include <ranges>

namespace tenzir {

namespace {

/// Create a `where` operator with the given expression.
auto make_where_ir(ast::expression filter) -> ir::operator_ptr {
  // TODO: This should just be a `std::make_unique<where_ir>(std::move(filter))`.
  const auto* where = plugins::find<operator_compiler_plugin>("tql2.where");
  TENZIR_ASSERT(where);
  auto args = std::vector<ast::expression>{};
  args.push_back(std::move(filter));
  // TODO: This is a terrible workaround. We are discarding diagnostics and
  // creating a new compile context, which should be created only once.
  auto dh = null_diagnostic_handler{};
  auto reg = global_registry();
  auto ctx = compile_ctx::make_root(base_ctx{dh, *reg});
  return where->compile(ast::invocation{ast::entity{{}}, std::move(args)}, ctx)
    .unwrap();
}

} // namespace

class if_exec final : public exec::operator_base {
public:
  if_exec() = default;

  if_exec(ast::expression condition, exec::pipeline then_, exec::pipeline else_)
    : condition_{std::move(condition)},
      then_{std::move(then_)},
      else_{std::move(else_)} {
  }

  auto name() const -> std::string override {
    return "if_exec";
  }

  auto spawn() const -> operator_actor override {
    TENZIR_TODO();
  }

  friend auto inspect(auto& f, if_exec& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition_),
                              f.field("then", x.then_),
                              f.field("else", x.else_));
  }

private:
  ast::expression condition_;
  exec::pipeline then_;
  exec::pipeline else_;
};

class if_ir final : public ir::operator_base {
public:
  struct else_t {
    location keyword;
    ir::pipeline pipe;

    friend auto inspect(auto& f, else_t& x) -> bool {
      return f.object(x).fields(f.field("keyword", x.keyword),
                                f.field("pipe", x.pipe));
    }
  };

  if_ir() = default;

  if_ir(location if_kw, ast::expression condition, ir::pipeline then,
        std::optional<else_t> else_)
    : if_kw_{if_kw},
      condition_{std::move(condition)},
      then_{std::move(then)},
      else_{std::move(else_)} {
  }

  auto name() const -> std::string override {
    return "if_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(condition_.substitute(ctx));
    TRY(then_.substitute(ctx, instantiate));
    if (else_) {
      TRY(else_->pipe.substitute(ctx, instantiate));
    }
    return {};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<exec::pipeline> override {
    TRY(auto then_instance, std::move(then_).finalize(ctx));
    auto else_instance = exec::pipeline{};
    if (else_) {
      TRY(else_instance, std::move(else_->pipe).finalize(ctx));
    }
    return std::make_unique<if_exec>(std::move(condition_),
                                     std::move(then_instance),
                                     std::move(else_instance));
  }

  auto infer_type(operator_type2 input, diagnostic_handler& dh) const
    -> failure_or<std::optional<operator_type2>> override {
    TRY(auto then_ty, then_.infer_type(input, dh));
    auto else_ty = std::optional{input};
    if (else_) {
      TRY(else_ty, else_->pipe.infer_type(input, dh));
    }
    if (not then_ty) {
      return else_ty;
    }
    if (not else_ty) {
      return then_ty;
    }
    if (*then_ty == *else_ty) {
      // TODO: One can also end in void.
      return then_ty;
    }
    // TODO: Improve diagnostic.
    diagnostic::error("incompatible branch output types: {} and {}",
                      operator_type_name(*then_ty),
                      operator_type_name(*else_ty))
      .primary(if_kw_)
      .emit(dh);
    return failure::promise();
  }

  friend auto inspect(auto& f, if_ir& x) -> bool {
    return f.object(x).fields(f.field("if_kw", x.if_kw_),
                              f.field("condition", x.condition_),
                              f.field("then", x.then_),
                              f.field("else", x.else_));
  }

private:
  location if_kw_;
  ast::expression condition_;
  ir::pipeline then_;
  std::optional<else_t> else_;
};

class legacy_exec final : public exec::operator_base {
public:
  legacy_exec() = default;

  explicit legacy_exec(operator_ptr op) : op_{std::move(op)} {
  }

  auto name() const -> std::string override {
    return "legacy_exec";
  }

  auto spawn(/*args*/) const -> operator_actor override {
    TENZIR_TODO();
  }

  friend auto inspect(auto& f, legacy_exec& x) -> bool {
    return plugin_inspect(f, x.op_);
  }

private:
  operator_ptr op_;
};

/// A wrapper for the previous operator API to make them work with the new IR.
class legacy_ir final : public ir::operator_base {
public:
  legacy_ir() = default;

  legacy_ir(location main_location, operator_ptr op)
    : main_location_{main_location}, state_{std::move(op)} {
  }

  legacy_ir(const operator_factory_plugin* plugin, ast::invocation inv)
    : main_location_{inv.op.get_location()},
      state_{partial{plugin, std::move(inv)}} {
  }

  auto name() const -> std::string override {
    return "legacy_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    auto state = try_as<partial>(state_);
    if (not state) {
      return {};
    }
    auto done = true;
    for (auto& arg : state->inv.args) {
      TRY(auto here, arg.substitute(ctx));
      done = done and here == ast::substitute_result::no_remaining;
    }
    if (not done) {
      TENZIR_ASSERT(not instantiate);
      return {};
    }
    auto provider = session_provider::make(ctx);
    TRY(state_, state->plugin->make(
                  operator_factory_plugin::invocation{
                    std::move(state->inv.op), std::move(state->inv.args)},
                  provider.as_session()));
    return {};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<exec::pipeline> override {
    (void)ctx;
    auto op = as<operator_ptr>(std::move(state_));
    if (auto pipe = dynamic_cast<pipeline*>(op.get())) {
      auto result = std::vector<exec::operator_ptr>{};
      for (auto& op : std::move(*pipe).unwrap()) {
        result.push_back(std::make_unique<legacy_exec>(std::move(op)));
      }
      return result;
    }
    return std::make_unique<legacy_exec>(std::move(op));
  }

  auto infer_type(operator_type2 input, diagnostic_handler& dh) const
    -> failure_or<std::optional<operator_type2>> override {
    auto op = try_as<operator_ptr>(state_);
    if (not op) {
      return std::nullopt;
    }
    auto legacy_input = match(input, [](auto x) -> operator_type {
      // TODO: This is where we could convert `chunk_ptr` types.
      return x;
    });
    auto legacy_output = (*op)->infer_type(legacy_input);
    if (not legacy_output) {
      // TODO: Refactor message?
      (legacy_input.is<void>()
         ? diagnostic::error("operator cannot be used as a source")
         : diagnostic::error("operator does not accept {}",
                             operator_type_name(legacy_input)))
        .primary(main_location_)
        .emit(dh);
      return failure::promise();
    }
    return match(*legacy_output, [](auto x) -> operator_type2 {
      return x;
    });
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    auto op = try_as<operator_ptr>(state_);
    if (not op) {
      return std::move(*this).operator_base::optimize(std::move(filter), order);
    }
    TENZIR_ASSERT(*op);
    auto legacy_conj = conjunction{};
    auto filter_rest = ir::optimize_filter{};
    for (auto& expr : filter) {
      auto [legacy, rest] = split_legacy_expression(expr);
      if (not is_true_literal(rest)) {
        filter_rest.push_back(std::move(rest));
      }
      if (legacy != trivially_true_expression()) {
        legacy_conj.push_back(std::move(legacy));
      }
    }
    auto legacy_expr = legacy_conj.empty()
                         ? trivially_true_expression()
                         : (legacy_conj.size() == 1 ? std::move(legacy_conj[0])
                                                    : std::move(legacy_conj));
    auto legacy_result = (*op)->optimize(legacy_expr, order);
    auto replacement = std::vector<ir::operator_ptr>{};
    replacement.emplace_back(std::make_unique<legacy_ir>(
      main_location_, std::move(legacy_result.replacement)));
    for (auto& expr : filter_rest) {
      replacement.push_back(make_where_ir(std::move(expr)));
    }
    // TODO: Transform this back into `ast::expression`.
    (void)legacy_result.filter;
    return ir::optimize_result{ir::optimize_filter{}, legacy_result.order,
                               ir::pipeline{{}, std::move(replacement)}};
  }

  auto main_location() const -> location override {
    return main_location_;
  }

  friend auto inspect(auto& f, legacy_ir& x) -> bool {
    return f.apply(x.state_);
  }

private:
  struct partial {
    const operator_factory_plugin* plugin;
    ast::invocation inv;

    friend auto inspect(auto& f, partial& x) -> bool {
      return f.object(x).fields(
        f.field(
          "plugin",
          [&]() {
            TENZIR_ASSERT(x.plugin);
            return x.plugin->name();
          },
          [&](std::string name) {
            x.plugin = plugins::find<operator_factory_plugin>(name);
            TENZIR_ASSERT(x.plugin);
            return true;
          }),
        f.field("inv", x.inv));
    }
  };

  location main_location_;
  variant<partial, operator_ptr> state_;
};

namespace {

// TODO: Clean this up. We might want to be able to just use
// `TENZIR_REGISTER_PLUGINS` also from `libtenzir` itself.
auto register_plugins_somewhat_hackily = std::invoke([]() {
  auto x = std::initializer_list<plugin*>{
    new inspection_plugin<ir::operator_base, legacy_ir>{},
    new inspection_plugin<exec::operator_base, legacy_exec>{},
    new inspection_plugin<ir::operator_base, if_ir>{},
    new inspection_plugin<exec::operator_base, if_exec>{},
  };
  for (auto y : x) {
    auto ptr = plugin_ptr::make_builtin(y,
                                        [](plugin* plugin) {
                                          delete plugin;
                                        },
                                        nullptr, {});
    const auto it = std::ranges::upper_bound(plugins::get_mutable(), ptr);
    plugins::get_mutable().insert(it, std::move(ptr));
  }
  return std::monostate{};
});

} // namespace

auto ast::pipeline::compile(compile_ctx ctx) && -> failure_or<ir::pipeline> {
  // TODO: Or do we assume that entities are already resolved?
  TRY(resolve_entities(*this, ctx));
  auto lets = std::vector<ir::let>{};
  auto operators = std::vector<ir::operator_ptr>{};
  auto scope = ctx.open_scope();
  for (auto& stmt : body) {
    auto result = match(
      stmt,
      [&](ast::invocation& x) -> failure_or<void> {
        auto& op = ctx.reg().get(x);
        return match(
          op.inner(),
          [&](const native_operator& op) -> failure_or<void> {
            if (not op.ir_plugin) {
              TENZIR_ASSERT(op.factory_plugin);
              for (auto& x : x.args) {
                // TODO: This doesn't work for operators which take
                // subpipelines... Should we just disallow subpipelines here?
                TRY(x.bind(ctx));
              }
              operators.emplace_back(
                std::make_unique<legacy_ir>(op.factory_plugin, std::move(x)));
              // TODO: Empty substitution?
              TRY(operators.back()->substitute(substitute_ctx{ctx, nullptr},
                                               false));
              return {};
            }
            // If there is a pipeline argument, we can't resolve `let`s in there
            // because the operator might introduce its own bindings. Thus, we
            // do not resolve any bindings, even when not in subpipelines. This
            // also gives the operator the option to accept let-bindings that
            // were not defined, for example because it can then introduce those
            // bindings by itself.
            TRY(auto compiled, op.ir_plugin->compile(x, ctx));
            TENZIR_ASSERT(compiled);
            operators.push_back(std::move(compiled));
            return {};
          },
          [&](const user_defined_operator& op) -> failure_or<void> {
            // TODO: What about diagnostics that end up being emitted here?
            // We need to provide a context that does not feature any outer
            // variables.
            auto udo_ctx = ctx.without_env();
            auto definition = op.definition;
            // By compiling the operator every time from AST to IR, we assign
            // new let IDs. This is important because if an operator is used
            // twice, it could have different values for its bindings.
            TRY(auto pipe, std::move(definition).compile(udo_ctx));
            // If it would have arguments, we need to create appropriate
            // bindings now. For constant arguments, we could bind the
            // parameters to a new `let` that stores that value. For
            // non-constant arguments, if we want to use the same `let`
            // mechanism, then we could introduce a new constant that can store
            // expressions that will be evaluated later.
            lets.insert(lets.end(), std::move_iterator{pipe.lets.begin()},
                        std::move_iterator{pipe.lets.end()});
            operators.insert(operators.end(),
                             std::move_iterator{pipe.operators.begin()},
                             std::move_iterator{pipe.operators.end()});
            return {};
          });
      },
      [&](ast::assignment& x) -> failure_or<void> {
        diagnostic::error("assignment is not implemented yet")
          .primary(x)
          .emit(ctx);
        return failure::promise();
      },
      [&](ast::let_stmt& x) -> failure_or<void> {
        TRY(x.expr.bind(ctx));
        auto id = scope.let(std::string{x.name_without_dollar()});
        lets.emplace_back(std::move(x.name), std::move(x.expr), id);
        return {};
      },
      [&](ast::if_stmt& x) -> failure_or<void> {
        TRY(x.condition.bind(ctx));
        TRY(auto then, std::move(x.then).compile(ctx));
        auto else_ = std::optional<if_ir::else_t>{};
        if (x.else_) {
          TRY(auto pipe, std::move(x.else_->pipe).compile(ctx));
          else_.emplace(x.else_->kw, std::move(pipe));
        }
        operators.emplace_back(std::make_unique<if_ir>(
          x.if_kw, std::move(x.condition), std::move(then), std::move(else_)));
        return {};
      },
      [&](ast::match_stmt& x) -> failure_or<void> {
        diagnostic::error("`match` is not implemented yet").primary(x).emit(ctx);
        return failure::promise();
      },
      [&](ast::type_stmt& x) -> failure_or<void> {
        diagnostic::error(
          "type declarations are not yet supported within pipelines")
          .primary(x.type_location)
          .emit(ctx);
        return failure::promise();
      });
    TRY(result);
  }
  return ir::pipeline{std::move(lets), std::move(operators)};
}

auto ir::pipeline::substitute(substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  if (instantiate) {
    auto env = ctx.env();
    for (auto& let : lets) {
      // We have to update every expression as we evaluate `let`s because later
      // bindings might reference earlier ones.
      TRY(auto subst, let.expr.substitute(ctx.with_env(&env)));
      TENZIR_ASSERT(subst == ast::substitute_result::no_remaining);
      TRY(auto value, const_eval(let.expr, ctx));
      // TODO: Clean this up. Should probably make `const_eval` return it.
      auto converted = match(
        value,
        [](auto& x) -> ast::constant::kind {
          return std::move(x);
        },
        [](pattern&) -> ast::constant::kind {
          TENZIR_UNREACHABLE();
        });
      auto inserted = env.try_emplace(let.id, std::move(converted)).second;
      TENZIR_ASSERT(inserted);
    }
    // Update each operator with the produced bindings.
    for (auto& op : operators) {
      TRY(op->substitute(ctx.with_env(&env), true));
    }
    // We don't need the lets anymore.
    lets.clear();
    return {};
  }
  // TODO: Do we still want to substitute deterministic bindings in here? Or
  // should that happen somewhere else? Could also help with type-checking.
  for (auto& let : lets) {
    TRY(let.expr.substitute(ctx));
  }
  for (auto& op : operators) {
    TRY(op->substitute(ctx, false));
  }
  return {};
}

auto ir::pipeline::finalize(finalize_ctx ctx) && -> failure_or<exec::pipeline> {
  // TODO: Assert that we were instantiated, or instantiate ourselves?
  TENZIR_ASSERT(lets.empty());
  auto opt = std::move(*this).optimize(optimize_filter{}, event_order::ordered);
  TENZIR_ASSERT(opt.replacement.lets.empty());
  // TODO: Should we really ignore this here?
  (void)opt.order;
  for (auto& expr : opt.filter) {
    opt.replacement.operators.insert(opt.replacement.operators.begin(),
                                     make_where_ir(expr));
  }
  *this = std::move(opt.replacement);
  auto result = std::vector<exec::operator_ptr>{};
  for (auto& op : operators) {
    TRY(auto ops, std::move(*op).finalize(ctx));
    result.insert(result.end(), std::move_iterator{ops.begin()},
                  std::move_iterator{ops.end()});
  }
  TENZIR_DIAGNOSTIC_PUSH
  TENZIR_DIAGNOSTIC_IGNORE_REDUNDANT_MOVE
  return std::move(result);
  TENZIR_DIAGNOSTIC_POP
}

auto ir::pipeline::infer_type(operator_type2 input,
                              diagnostic_handler& dh) const
  -> failure_or<std::optional<operator_type2>> {
  for (auto& op : operators) {
    TRY(auto output, op->infer_type(input, dh));
    TRY(input, output);
  }
  return input;
}

auto ir::pipeline::optimize(optimize_filter filter,
                            event_order order) && -> optimize_result {
  auto replacement = pipeline{std::move(lets), {}};
  for (auto& op : std::ranges::reverse_view(operators)) {
    auto opt = std::move(*op).optimize(std::move(filter), order);
    filter = std::move(opt.filter);
    order = opt.order;
    replacement.operators.insert(
      replacement.operators.begin(),
      std::move_iterator{opt.replacement.operators.begin()},
      std::move_iterator{opt.replacement.operators.end()});
  }
  return {std::move(filter), order, std::move(replacement)};
}

auto ir::operator_base::optimize(optimize_filter filter,
                                 event_order order) && -> optimize_result {
  (void)order;
  auto replacement = std::vector<operator_ptr>{};
  replacement.push_back(std::move(*this).move());
  for (auto& expr : filter) {
    replacement.push_back(make_where_ir(expr));
  }
  return {optimize_filter{}, event_order::ordered,
          pipeline{{}, std::move(replacement)}};
}

auto ir::operator_base::copy() const -> operator_ptr {
  auto p = plugins::find<serialization_plugin<operator_base>>(name());
  if (not p) {
    TENZIR_ERROR("could not find serialization plugin `{}`", name());
    TENZIR_ASSERT(false);
  }
  auto buffer = caf::byte_buffer{};
  auto f = caf::binary_serializer{buffer};
  auto success = p->serialize(f, *this);
  if (not success) {
    TENZIR_ERROR("failed to serialize `{}` operator: {}", name(),
                 f.get_error());
    TENZIR_ASSERT(false);
  }
  auto g = caf::binary_deserializer{buffer};
  auto copy = std::unique_ptr<operator_base>{};
  p->deserialize(g, copy);
  if (not copy) {
    TENZIR_ERROR("failed to deserialize `{}` operator: {}", name(),
                 g.get_error());
    TENZIR_ASSERT(false);
  }
  return copy;
}

auto ir::operator_base::move() && -> operator_ptr {
  // TODO: This should be overriden by something like CRTP.
  return copy();
}

auto ir::operator_base::infer_type(operator_type2 input,
                                   diagnostic_handler& dh) const
  -> failure_or<std::optional<operator_type2>> {
  // TODO: Is this a good default to have? Should probably be pure virtual.
  (void)input, (void)dh;
  return std::nullopt;
}

auto operator_compiler_plugin::operator_name() const -> std::string {
  auto result = name();
  if (result.starts_with("tql2.")) {
    result = result.substr(5);
  }
  return result;
}

} // namespace tenzir
