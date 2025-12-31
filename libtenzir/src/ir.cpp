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
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/rebatch.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/tql2/user_defined_operator.hpp"

#include <ranges>

namespace tenzir {

namespace {

/// Create a `where` operator with the given expression.
auto make_where_ir(ast::expression filter) -> Box<ir::Operator> {
  // TODO: This should just be a `where_ir{std::move(filter)}`.
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

class Set final : public Operator<table_slice, table_slice> {
public:
  Set(std::vector<ast::assignment> assignments, event_order order)
    : assignments_{std::move(assignments)}, order_{order} {
    for (auto& assignment : assignments_) {
      auto [pruned_assignment, moved_fields]
        = resolve_move_keyword(std::move(assignment));
      assignment = std::move(pruned_assignment);
      std::ranges::move(moved_fields, std::back_inserter(moved_fields_));
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> {
    auto slice = std::move(input);
    // The right-hand side is always evaluated with the original input, because
    // side-effects from preceding assignments shall not be reflected when
    // calculating the value of the left-hand side.
    auto values = std::vector<multi_series>{};
    for (const auto& assignment : assignments_) {
      values.push_back(eval(assignment.right, slice, ctx));
    }
    slice = drop(slice, moved_fields_, ctx, false);
    // After we know all the multi series values on the right, we can split the
    // input table slice and perform the actual assignment.
    auto begin = int64_t{0};
    auto results = std::vector<table_slice>{};
    for (auto values_slice : split_multi_series(values)) {
      TENZIR_ASSERT(not values_slice.empty());
      auto end = begin + values_slice[0].length();
      // We could still perform further splits if metadata is assigned.
      auto state = std::vector<table_slice>{};
      state.push_back(subslice(slice, begin, end));
      begin = end;
      auto new_state = std::vector<table_slice>{};
      for (auto [assignment, value] :
           detail::zip_equal(assignments_, values_slice)) {
        auto begin = int64_t{0};
        for (auto& entry : state) {
          auto entry_rows = detail::narrow<int64_t>(entry.rows());
          auto assigned = assign(assignment.left,
                                 value.slice(begin, entry_rows), entry, ctx);
          begin += entry_rows;
          new_state.insert(new_state.end(),
                           std::move_iterator{assigned.begin()},
                           std::move_iterator{assigned.end()});
        }
        std::swap(state, new_state);
        new_state.clear();
      }
      std::ranges::move(state, std::back_inserter(results));
    }
    // TODO: Consider adding a property to function plugins that let's them
    // indicate whether they want their outputs to be strictly ordered. If any
    // of the called functions has this requirement, then we should not be
    // making this optimization. This will become relevant in the future once we
    // allow functions to be stateful.
    if (order_ != event_order::ordered) {
      std::ranges::stable_sort(results, std::ranges::less{},
                               &table_slice::schema);
    }
    for (auto& result : rebatch(std::move(results))) {
      co_await push(std::move(result));
    }
  }

private:
  std::vector<ast::assignment> assignments_;
  event_order order_ = event_order::ordered;
  std::vector<ast::field_path> moved_fields_;
};

class set_ir final : public ir::Operator {
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

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return Set{std::move(assignments_), order_};
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // Remember the order for potential rebatches.
    order_ = order;
    auto ops = std::vector<Box<ir::Operator>>{};
    if (not filter.empty()) {
      // TODO: FIXME
      TENZIR_ASSERT(filter.size() == 1);
      ops.reserve(2);
      auto where = make_where_ir(filter[0]);
      ops.push_back(std::move(where));
    }
    ops.emplace_back(set_ir{std::move(*this)});
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
auto make_set_ir(ast::assignment x) -> Box<ir::Operator> {
  auto assignments = std::vector<ast::assignment>{};
  assignments.push_back(std::move(x));
  return set_ir{std::move(assignments)};
}

} // namespace

class if_ir final : public ir::Operator {
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

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    // TODO: Implement If operator that handles subpipelines.
    TENZIR_TODO();
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

namespace {

// TODO: Clean this up. We might want to be able to just use
// `TENZIR_REGISTER_PLUGINS` also from `libtenzir` itself.
auto register_plugins_somewhat_hackily = std::invoke([]() {
  auto x = std::initializer_list<plugin*>{
    new inspection_plugin<ir::Operator, if_ir>{},
    new inspection_plugin<ir::Operator, set_ir>{},
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
  auto operators = std::vector<Box<ir::Operator>>{};
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
// FIXME: Decider whether to make a hard cut or not.
#if 0
              TENZIR_ASSERT(op.factory_plugin);
              for (auto& x : x.args) {
                // TODO: This doesn't work for operators which take
                // subpipelines... Should we just disallow subpipelines here?
                TRY(x.bind(ctx));
              }
              operators.emplace_back(
                legacy_ir{op.factory_plugin, std::move(x))};
              // TODO: Empty substitution?
              TRY(operators.back()->substitute(substitute_ctx{ctx, nullptr},
                                               false));
              return {};
#else
              diagnostic::error("this operator was not ported yet")
                .primary(x.op)
                .emit(ctx);
              return failure::promise();
#endif
            }
            // If there is a pipeline argument, we can't resolve `let`s in there
            // because the operator might introduce its own bindings. Thus, we
            // do not resolve any bindings, even when not in subpipelines. This
            // also gives the operator the option to accept let-bindings that
            // were not defined, for example because it can then introduce those
            // bindings by itself.
            TRY(auto compiled, op.ir_plugin->compile(x, ctx));
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
        // TODO: What about left?
        TRY(x.right.bind(ctx));
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
        operators.emplace_back(if_ir{x.if_kw, std::move(x.condition),
                                     std::move(then), std::move(else_)});
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

auto ir::pipeline::spawn(element_type_tag input) && -> std::vector<AnyOperator> {
  // TODO: Assert that we were instantiated, or instantiate ourselves?
  TENZIR_ASSERT(lets.empty());
  // TODO: This is probably not the right place for optimizations.
  auto opt = std::move(*this).optimize(optimize_filter{}, event_order::ordered);
  TENZIR_ASSERT(opt.replacement.lets.empty());
  // TODO: Should we really ignore this here?
  (void)opt.order;
  for (auto& expr : opt.filter) {
    opt.replacement.operators.insert(opt.replacement.operators.begin(),
                                     make_where_ir(expr));
  }
  *this = std::move(opt.replacement);
  auto result = std::vector<AnyOperator>{};
  for (auto& op : operators) {
    // We already checked, there should be no diagnostics here.
    auto dh = null_diagnostic_handler{};
    auto output = op->infer_type(input, dh);
    TENZIR_ASSERT(output);
    TENZIR_ASSERT(*output);
    result.push_back(std::move(*op).spawn(input));
    input = **output;
  }
  return result;
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

auto ir::Operator::optimize(optimize_filter filter,
                            event_order order) && -> optimize_result {
  (void)order;
  auto replacement = std::vector<Box<Operator>>{};
  replacement.push_back(std::move(*this).move());
  for (auto& expr : filter) {
    replacement.push_back(make_where_ir(expr));
  }
  return {optimize_filter{}, event_order::ordered,
          pipeline{{}, std::move(replacement)}};
}

auto ir::Operator::copy() const -> Box<Operator> {
  auto p = plugins::find<serialization_plugin<Operator>>(name());
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
  auto copy = std::unique_ptr<ir::Operator>{};
  p->deserialize(g, copy);
  if (not copy) {
    TENZIR_ERROR("failed to deserialize `{}` operator: {}", name(),
                 g.get_error());
    TENZIR_ASSERT(false);
  }
  return Box<Operator>::from_unique_ptr(std::move(copy));
}

auto ir::Operator::move() && -> Box<Operator> {
  // TODO: This should be overriden by something like CRTP.
  return copy();
}

auto ir::Operator::infer_type(element_type_tag input,
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

} // namespace tenzir
