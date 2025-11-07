//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/exec/checkpoint.hpp"
#include "tenzir/exec/operator_base.hpp"
#include "tenzir/execution_node.hpp"
#include "tenzir/finalize_ctx.hpp"
#include "tenzir/plan/operator_spawn_args.hpp"
#include "tenzir/plan/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/report.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <caf/actor_from_state.hpp>

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

class stateless_transform_operator {};

class set_exec : public exec::operator_base {
public:
  set_exec(exec::operator_actor::pointer self,
           std::vector<ast::assignment> assignments, event_order order)
    : exec::operator_base{self},
      assignments_{std::move(assignments)},
      order_{order} {
  }

  void on_push(table_slice slice) override {
    // TODO: Perform the actual assignment.
    push(std::move(slice));
  }
  void on_push(chunk_ptr chunk) override {
    TENZIR_TODO();
  }
  auto serialize() -> chunk_ptr override {
  }
  void on_done() override {
    finish();
  }
  void on_pull(uint64_t items) {
  }

private:
  std::vector<ast::assignment> assignments_;
  event_order order_;
};

class set_plan final : public plan::operator_base {
public:
  explicit set_plan(std::vector<ast::assignment> assignments)
    : assignments_{std::move(assignments)} {
  }

  auto name() const -> std::string override {
    return "set_plan";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    return args.sys.spawn(caf::actor_from_state<set_exec>, assignments_,
                          order_);
  }

  friend auto inspect(auto& f, set_plan& x) -> bool {
    return f.object(x).fields(f.field("assignments", x.assignments_));
  }

private:
  std::vector<ast::assignment> assignments_;
  event_order order_ = event_order::ordered;
};

class set_ir final : public ir::operator_base {
public:
  set_ir() = default;

  explicit set_ir(std::vector<ast::assignment> assignments)
    : assignments_(std::move(assignments)) {
  }

  auto name() const -> std::string override {
    return "set_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    (void)instantiate;
    for (auto& x : assignments_) {
      TRY(x.right.substitute(ctx));
    }
    return {};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    return std::make_unique<set_plan>(std::move(assignments_));
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // Remember the order for potential rebatches.
    order_ = order;
    // TODO: FIXME
    TENZIR_ASSERT(filter.size() == 1);
    auto where = make_where_ir(filter[0]);
    auto ops = std::vector<ir::operator_ptr>{};
    ops.reserve(2);
    ops.push_back(std::move(where));
    ops.emplace_back(std::make_unique<set_ir>(std::move(*this)));
    auto replacement = ir::pipeline{std::vector<ir::let>{}, std::move(ops)};
    return {{}, order_, std::move(replacement)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("set operator expected events").emit(dh);
      return failure::promise();
    }
    return input;
  }

  friend auto inspect(auto& f, set_ir& x) -> bool {
    return f.object(x).fields(f.field("assignments", x.assignments_));
  }

private:
  std::vector<ast::assignment> assignments_;
  event_order order_ = event_order::ordered;
};

/// Create a `set` operator with the given assignment.
auto make_set_ir(ast::assignment x) -> ir::operator_ptr {
  auto assignments = std::vector<ast::assignment>{};
  assignments.push_back(std::move(x));
  return std::make_unique<set_ir>(std::move(assignments));
}

} // namespace

class if_exec final : public plan::operator_base {
public:
  if_exec() = default;

  if_exec(ast::expression condition, plan::pipeline then_, plan::pipeline else_)
    : condition_{std::move(condition)},
      then_{std::move(then_)},
      else_{std::move(else_)} {
  }

  auto name() const -> std::string override {
    return "if_exec";
  }

  auto spawn(plan::operator_spawn_args) const -> exec::operator_actor override {
    TENZIR_TODO();
  }

  friend auto inspect(auto& f, if_exec& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition_),
                              f.field("then", x.then_),
                              f.field("else", x.else_));
  }

private:
  ast::expression condition_;
  plan::pipeline then_;
  plan::pipeline else_;
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

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    TRY(auto then_instance, std::move(then_).finalize(ctx));
    auto else_instance = plan::pipeline{};
    if (else_) {
      TRY(else_instance, std::move(else_->pipe).finalize(ctx));
    }
    return std::make_unique<if_exec>(std::move(condition_),
                                     std::move(then_instance),
                                     std::move(else_instance));
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
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

class legacy_metrics_receiver {
public:
  auto make_behavior() -> metrics_receiver_actor::behavior_type {
    return {
      [this](uint64_t op_index, uuid metrics_id, type) -> caf::result<void> {
        // TODO
        TENZIR_TODO();
      },
      [this](uint64_t op_index, uuid metrics_id, record) -> caf::result<void> {
        // TODO
        TENZIR_TODO();
      },
      [this](const operator_metric& metric) -> caf::result<void> {
        // TODO
        TENZIR_TODO();
      },
    };
  }
};

class legacy_control_plane final : public operator_control_plane {
public:
  explicit legacy_control_plane(exec_node_actor::base& self) {
  }

  auto self() noexcept -> exec_node_actor::base& override {
    TENZIR_TODO();
  }

  auto definition() const noexcept -> std::string_view override {
    TENZIR_TODO();
  }

  auto run_id() const noexcept -> uuid override {
    TENZIR_TODO();
  }

  auto node() noexcept -> node_actor override {
    TENZIR_TODO();
  }

  auto operator_index() const noexcept -> uint64_t override {
    TENZIR_TODO();
  }

  auto diagnostics() noexcept -> diagnostic_handler& override {
    TENZIR_TODO();
  }

  auto metrics(type t) noexcept -> metric_handler override {
    return metric_handler{
      self().spawn(caf::actor_from_state<legacy_metrics_receiver>), 0, t};
  }

  auto metrics_receiver() const noexcept -> metrics_receiver_actor override {
    TENZIR_TODO();
  }

  auto no_location_overrides() const noexcept -> bool override {
    TENZIR_TODO();
  }

  auto has_terminal() const noexcept -> bool override {
    TENZIR_TODO();
  }

  auto is_hidden() const noexcept -> bool override {
    TENZIR_TODO();
  }

  auto set_waiting(bool value) noexcept -> void override {
    TENZIR_TODO();
  }

  auto resolve_secrets_must_yield(std::vector<secret_request> requests,
                                  final_callback_t final_callback)
    -> secret_resolution_sentinel override {
    TENZIR_TODO();
  }
};

struct legacy_exec_trait {
  using signatures
    = exec::operator_actor::signatures::append_from<exec_node_actor::signatures>;
};

using legacy_exec_actor = caf::typed_actor<legacy_exec_trait>;

// TODO: This cannot be final because of CAF...
class legacy_exec : public exec::basic_operator<legacy_exec_actor> {
public:
  static constexpr auto name = "legacy-exec";

  legacy_exec(legacy_exec_actor::pointer self, operator_ptr op, base_ctx ctx)
    : basic_operator{self}, op_{std::move(op)}, ctx_{ctx} {
    auto exec_node
      = spawn_exec_node(self_, op_->copy(), operator_type::of<table_slice>,
                        "TODO: definition", node_actor{},
                        receiver_actor<diagnostic>{}, metrics_receiver_actor{},
                        /*index*/ 0, false, false, uuid::random());
    auto [actor, output_type] = check(exec_node);
    // TODO: Pass `self_` as previous if it's not a source.
    self_->mail(atom::start_v, std::vector<caf::actor>{})
      .request(actor, caf::infinite)
      .then([] {}, TENZIR_REPORT);
    // TODO: Numbers.
    self_
      ->mail(atom::pull_v, exec_node_sink_actor{self_}, uint64_t{10},
             uint64_t{10})
      .request(actor, caf::infinite)
      .then([] {}, TENZIR_REPORT);
  }

  auto make_behavior() -> legacy_exec_actor::behavior_type {
    return extend_behavior(std::tuple{
      [this](atom::start,
             std::vector<caf::actor> all_previous) -> caf::result<void> {
        TENZIR_ASSERT(all_previous.empty());
      },
      [this](atom::pause) -> caf::result<void> {
        return {};
      },
      [this](atom::resume) -> caf::result<void> {
        return {};
      },
      [this](diagnostic diag) -> caf::result<void> {
        std::move(diag).modify().emit(ctx_);
        return {};
      },
      [this](atom::pull, exec_node_sink_actor sink, uint64_t elements,
             uint64_t batches) -> caf::result<void> {
        pull_rp_ = self_->make_response_promise<void>();
        pull(elements);
        return {};
      },
      [this](atom::push, table_slice events) -> caf::result<void> {
        push(std::move(events));
        return {};
      },
      [this](atom::push, chunk_ptr bytes) -> caf::result<void> {
        push(std::move(bytes));
        return {};
      },
    });
  }

  auto on_start() -> caf::result<void> override {
    return {};
  }

  void on_push(table_slice slice) override {
    // TODO: Schedule execution?
    // TODO: Check.
    // as<std::deque<table_slice>>(input_).push_back(std::move(slice));
    // run_once();
  }

  void on_push(chunk_ptr chunk) override {
    // as<std::deque<chunk_ptr>>(input_).push_back(std::move(chunk));
    // run_once();
  }

  void on_pull(uint64_t items) override {
    // TODO: Can we ignore demand here?
  }

  auto serialize() -> chunk_ptr override {
    return {};
  }

  void on_done() override {
  }

private:
  operator_ptr op_;
  base_ctx ctx_;
  caf::typed_response_promise<void> pull_rp_;
};

class legacy_plan final : public plan::operator_base {
public:
  legacy_plan() = default;

  explicit legacy_plan(operator_ptr op) : op_{std::move(op)} {
  }

  auto name() const -> std::string override {
    return "legacy_plan";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    return args.sys.spawn(caf::actor_from_state<legacy_exec>, op_->copy(),
                          args.ctx);
  }

  friend auto inspect(auto& f, legacy_plan& x) -> bool {
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

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    (void)ctx;
    auto op = as<operator_ptr>(std::move(state_));
    if (auto pipe = dynamic_cast<pipeline*>(op.get())) {
      auto result = std::vector<plan::operator_ptr>{};
      for (auto& op : std::move(*pipe).unwrap()) {
        result.push_back(std::make_unique<legacy_plan>(std::move(op)));
      }
      return result;
    }
    return std::make_unique<legacy_plan>(std::move(op));
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
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
    return match(*legacy_output, [](auto x) -> element_type_tag {
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
    new inspection_plugin<plan::operator_base, legacy_plan>{},
    new inspection_plugin<ir::operator_base, if_ir>{},
    new inspection_plugin<plan::operator_base, if_exec>{},
    new inspection_plugin<ir::operator_base, set_ir>{},
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
      std::move(stmt),
      [&](ast::invocation x) -> failure_or<void> {
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
      [&](ast::assignment x) -> failure_or<void> {
        operators.push_back(make_set_ir(std::move(x)));
        return {};
      },
      [&](ast::let_stmt x) -> failure_or<void> {
        TRY(x.expr.bind(ctx));
        auto id = scope.let(std::string{x.name_without_dollar()});
        lets.emplace_back(std::move(x.name), std::move(x.expr), id);
        return {};
      },
      [&](ast::if_stmt x) -> failure_or<void> {
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
      [&](ast::match_stmt x) -> failure_or<void> {
        diagnostic::error("`match` is not implemented yet").primary(x).emit(ctx);
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

auto ir::pipeline::finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> {
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
  auto result = std::vector<plan::operator_ptr>{};
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

auto ir::pipeline::infer_type(element_type_tag input,
                              diagnostic_handler& dh) const
  -> failure_or<std::optional<element_type_tag>> {
  for (auto& op : operators) {
    TRY(auto output, op->infer_type(input, dh));
    TRY(input, output);
    // TODO: What if we get void in the middle?
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

auto ir::operator_base::infer_type(element_type_tag input,
                                   diagnostic_handler& dh) const
  -> failure_or<std::optional<element_type_tag>> {
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

ir::operator_ptr::operator_ptr(const operator_ptr& other) {
  *this = other;
}

auto ir::operator_ptr::operator=(const operator_ptr& other) -> operator_ptr& {
  *this = other->copy();
  return *this;
}

} // namespace tenzir
