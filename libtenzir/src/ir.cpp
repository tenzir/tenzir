//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/ir_match.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/rebatch.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/tql2/user_defined_operator.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view3.hpp"

#include <algorithm>
#include <ranges>
#include <thread>
#include <utility>

namespace tenzir {

auto ir::split_filter_by_dependents(ir::optimize_filter filter,
                                    const ast::ExprRefs& touched)
  -> ir::split_filter_result {
  auto result = ir::split_filter_result{};
  if (touched.let_ids.empty() and touched.field_paths.empty()) {
    result.independent = std::move(filter);
    return result;
  }
  for (auto& expr : filter) {
    auto refs = ast::collect_refs(expr);
    if (not refs or refs->overlaps(touched)) {
      result.dependent.push_back(std::move(expr));
    } else {
      result.independent.push_back(std::move(expr));
    }
  }
  return result;
}

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
  auto compiled
    = where->compile(ast::invocation{ast::entity{{}}, std::move(args)}, ctx)
        .unwrap();
  auto pipe = std::move(compiled).unwrap();
  TENZIR_ASSERT(pipe.lets.empty());
  TENZIR_ASSERT_EQ(pipe.operators.size(), 1);
  return std::move(pipe.operators.front());
}

namespace {

// Migration hints for the neo executor transition.
struct porting_hint {
  std::string_view legacy_name;
  std::string_view message;
};

constexpr porting_hint unported_replacements[] = {
  {"compress", "use one of the `compress_*` operators (e.g. `compress_gzip`, "
               "`compress_zstd`) instead"},
  {"decompress", "use one of the `decompress_*` operators (e.g. "
                 "`decompress_gzip`, "
                 "`decompress_zstd`) instead"},
  {"from", "use one of the `from_*` operators (e.g. `from_file`, "
           "`from_http`) "
           "instead"},
  {"from_gcs", "use `from_google_cloud_storage` instead"},
  {"from_sqs", "use `from_amazon_sqs` instead"},
  {"from_udp", "use `accept_udp` instead"},
  {"http", "use `from_http` instead, combined with `each` if needed"},
  {"load_amqp", "use `from_amqp` instead"},
  {"load_azure_blob_storage", "use `from_azure_blob_storage` instead"},
  {"load_file", "use `from_file` instead"},
  {"load_gcs", "use `from_google_cloud_storage` instead"},
  {"load_google_cloud_pubsub", "use `from_google_cloud_pubsub` instead"},
  {"load_kafka", "use `from_kafka` instead"},
  {"load_nic", "use `from_nic` instead"},
  {"load_s3", "use `from_s3` instead"},
  {"load_stdin", "use `from_stdin` instead"},
  {"load_sqs", "use `from_amazon_sqs` instead"},
  {"load_tcp", "use `accept_tcp` instead"},
  {"load_zmq", "use `from_zmq` instead"},
  {"move", "use the `dst = move src` keyword form instead"},
  {"save_amqp", "use `to_amqp` instead"},
  {"save_azure_blob_storage", "use `to_azure_blob_storage` instead"},
  {"save_file", "use `to_file` instead"},
  {"save_gcs", "use `to_google_cloud_storage` instead"},
  {"save_google_cloud_pubsub", "use `to_google_cloud_pubsub` instead"},
  {"save_kafka", "use `to_kafka` instead"},
  {"save_s3", "use `to_s3` instead"},
  {"save_stdout", "use `to_stdout` instead"},
  {"save_sqs", "use `to_amazon_sqs` instead"},
  {"save_zmq", "use `to_zmq` instead"},
  {"to", "use one of the `to_*` operators (e.g. `to_file`, `to_http`) "
         "instead"},
  {"to_sqs", "use `to_amazon_sqs` instead"},
  {"to_hive",
   "use `to_file`, `to_s3`, etc. with the `partition_by` argument instead"},
};

auto get_porting_hint(const ast::entity& op) -> std::string_view {
  const auto it = std::ranges::find(unported_replacements, op.path.back().name,
                                    &porting_hint::legacy_name);
  return it != std::ranges::end(unported_replacements) ? it->message
                                                       : std::string_view{};
}

auto merge_compiled_pipeline(std::vector<ir::let>& lets,
                             std::vector<Box<ir::Operator>>& operators,
                             ir::pipeline pipe) -> void {
  lets.insert(lets.end(), std::move_iterator{pipe.lets.begin()},
              std::move_iterator{pipe.lets.end()});
  operators.insert(operators.end(), std::move_iterator{pipe.operators.begin()},
                   std::move_iterator{pipe.operators.end()});
}

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
    // Compilation rejects assignment targets that do not describe a selector,
    // so the conversion cannot fail here anymore.
    lefts_.reserve(assignments_.size());
    for (const auto& assignment : assignments_) {
      auto left = ast::selector::try_from(assignment.left);
      TENZIR_ASSERT(left);
      lefts_.push_back(std::move(*left));
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
      for (auto [left, value] : std::views::zip(lefts_, values_slice)) {
        auto begin = int64_t{0};
        for (auto& entry : state) {
          auto entry_rows = detail::narrow<int64_t>(entry.rows());
          auto assigned
            = assign(left, value.slice(begin, entry_rows), entry, ctx);
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
  std::vector<ast::selector> lefts_;
  event_order order_{};
  std::vector<ast::field_path> moved_fields_;
};

} // namespace

ir::SetIr::SetIr() : order_{event_order::ordered} {
}

ir::SetIr::SetIr(std::vector<ast::assignment> assignments)
  : assignments_{std::move(assignments)}, order_{event_order::ordered} {
}

auto ir::SetIr::name() const -> std::string {
  return "SetIr";
}

auto ir::SetIr::copy() const -> Box<ir::Operator> {
  return SetIr{*this};
}

auto ir::SetIr::move() && -> Box<ir::Operator> {
  return SetIr{std::move(*this)};
}

auto ir::SetIr::substitute(substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  (void)instantiate;
  for (auto& x : assignments_) {
    // The left-hand side is resolved to a selector at compile time and cannot
    // contain `$`-variables. UDO parameters are resolved even before that.
    TRY(x.right.substitute(ctx));
  }
  return {};
}

auto ir::SetIr::spawn(element_type_tag input) const -> AnyOperator {
  TENZIR_ASSERT(input.is<table_slice>());
  return Set{assignments_, order_}.with_name("set");
}

namespace {

auto touched_fields_for_set(const std::vector<ast::assignment>& assignments)
  -> Option<std::vector<ast::field_path>> {
  auto result = std::vector<ast::field_path>{};
  for (const auto& assignment : assignments) {
    auto [resolved, moved_fields] = resolve_move_keyword(assignment);
    std::ranges::move(moved_fields, std::back_inserter(result));
    auto left = ast::selector::try_from(resolved.left);
    const auto* path = left ? try_as<ast::field_path>(&*left) : nullptr;
    if (path == nullptr or path->path().empty()) {
      return None{};
    }
    result.push_back(*path);
  }
  return result;
}

} // namespace

auto ir::SetIr::optimize(ir::optimize_filter filter,
                         event_order order) && -> ir::optimize_result {
  order_ = weaker_event_order(order_, order);
  auto touched_paths = touched_fields_for_set(assignments_);
  auto split = touched_paths
                 ? ir::split_filter_by_dependents(
                     std::move(filter),
                     ast::ExprRefs{.field_paths = std::move(*touched_paths)})
                 : ir::split_filter_result{{}, std::move(filter)};
  auto [filter_upstream, filter_self] = std::move(split);
  auto ops = std::vector<Box<ir::Operator>>{};
  ops.reserve(1 + filter_self.size());
  ops.emplace_back(ir::SetIr{std::move(*this)});
  for (auto& expr : filter_self) {
    ops.push_back(make_where_ir(expr));
  }
  return {
    std::move(filter_upstream),
    order_,
    ir::pipeline{{}, std::move(ops)},
  };
}

auto ir::SetIr::infer_type(element_type_tag input, diagnostic_handler& dh) const
  -> failure_or<element_type_tag> {
  if (input.is_not<table_slice>()) {
    diagnostic::error("set operator expected events").emit(dh);
    return failure::promise();
  }
  return input;
}

namespace ir {

template <class Inspector>
auto inspect(Inspector& f, SetIr& x) -> bool {
  return f.object(x).fields(f.field("assignments", x.assignments_),
                            f.field("order", x.order_));
}

} // namespace ir

namespace {

/// Create a `set` operator with the given assignment.
auto make_set_ir(ast::assignment x) -> Box<ir::Operator> {
  auto assignments = std::vector<ast::assignment>{};
  assignments.push_back(std::move(x));
  return ir::SetIr{std::move(assignments)};
}

struct IfArgs {
  ast::expression condition;
  ir::pipeline consequence;
  std::optional<ir::pipeline> alternative;

  friend auto inspect(auto& f, IfArgs& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition),
                              f.field("consequence", x.consequence),
                              f.field("alternative", x.alternative));
  }
};

} // namespace

auto make_set_ir(std::vector<ast::assignment> assignments)
  -> Box<ir::Operator> {
  return ir::SetIr{std::move(assignments)};
}

namespace {

class IfIr final : public ir::Operator {
public:
  IfIr() = default;

  explicit IfIr(IfArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "If";
  }

  auto copy() const -> Box<ir::Operator> override {
    return IfIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return IfIr{std::move(args_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(args_.condition.substitute(ctx));
    TRY(args_.consequence.substitute(ctx, instantiate));
    if (args_.alternative) {
      TRY(args_.alternative->substitute(ctx, instantiate));
    }
    return {};
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // We need to skip `-> void` pipelines, which are invalid to optimize with
    // the downstream filter.
    auto null_dh = null_diagnostic_handler{};
    auto outputs_events = [&](ir::pipeline const& pipe) -> bool {
      auto t = pipe.infer_type(tag_v<table_slice>, null_dh);
      return t and (*t).is<table_slice>();
    };
    auto optimize_branch
      = [&](ir::pipeline& branch, ir::optimize_filter f) -> event_order {
      auto opt = std::move(branch).optimize(std::move(f), order);
      branch = std::move(opt.replacement);
      branch.operators.insert_range(branch.operators.begin(),
                                    opt.filter
                                      | std::views::transform(make_where_ir));
      return opt.order;
    };
    // Handle downstream filters when there is no explicit `else` branch.
    if (not args_.alternative and not filter.empty()) {
      args_.alternative.emplace(ir::pipeline{});
    }
    auto cons_filter
      = outputs_events(args_.consequence) ? filter : ir::optimize_filter{};
    auto cons_order
      = optimize_branch(args_.consequence, std::move(cons_filter));
    auto alt_order = order;
    if (args_.alternative) {
      auto alt_filter = outputs_events(*args_.alternative)
                          ? std::move(filter)
                          : ir::optimize_filter{};
      alt_order = optimize_branch(*args_.alternative, std::move(alt_filter));
    }
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      {},
      stronger_event_order(cons_order, alt_order),
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    // `if` expands into the plan via `plan()` and is never spawned as a single
    // node.
    TENZIR_UNREACHABLE();
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    // Flatten `if` into a broadcast that copies the input to two branches, each
    // guarded by a `where`: the consequence keeps rows where the condition is
    // `true`, the alternative keeps the rest. Without an explicit `else`, the
    // alternative forwards the unmatched rows unchanged.
    auto src = builder.into_single(input);
    auto heads = std::vector<size_t>{};
    auto tails = ir::PlanPorts{};
    auto lower_branch
      = [&](ir::pipeline branch, ast::expression filter) -> failure_or<void> {
      branch.operators.insert(branch.operators.begin(),
                              make_where_ir(std::move(filter)));
      auto ty = tag_v<table_slice>;
      auto head = builder.add_identity(ty);
      heads.push_back(head);
      TRY(auto tail,
          builder.lower_pipeline(std::move(branch),
                                 ir::PlanPorts{ir::PlanPort{head, ty}}, dh));
      tails.insert(tails.end(), tail.begin(), tail.end());
      return {};
    };
    // Consequence: `where <cond>` keeps rows where the condition is `true` and
    // emits the sole `expected bool` diagnostic for null/non-bool conditions.
    TRY(lower_branch(std::move(args_.consequence), args_.condition));
    // Alternative: `(not <cond>) else true` keeps the rest (`false` and null
    // route to the else branch, matching `if`'s partition). Coalescing the
    // negation's null result to `true` routes null rows here without dropping
    // them and without emitting a second `expected bool` diagnostic.
    const auto loc = args_.condition.get_location();
    auto guard = ast::expression{ast::binary_expr{
      ast::expression{
        ast::unary_expr{located{ast::unary_op::not_, loc}, args_.condition}},
      ast::binary_op::else_, ast::expression{ast::constant{true, loc}}}};
    auto alternative
      = args_.alternative ? std::move(*args_.alternative) : ir::pipeline{};
    TRY(lower_branch(std::move(alternative), std::move(guard)));
    builder.add_broadcast({src}, std::move(heads));
    return tails;
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    // A branch may be empty (or contain only `let`s), in which case it has no
    // operator to point at. Fall back to the condition's location, which is
    // always present.
    auto branch_location = [&](const ir::pipeline& branch) -> location {
      if (not branch.operators.empty()) {
        return branch.operators.back()->main_location();
      }
      return args_.condition.get_location();
    };
    TRY(auto then_ty, args_.consequence.infer_type(input, dh));
    auto else_ty = input;
    if (args_.alternative) {
      TRY(else_ty, args_.alternative->infer_type(input, dh));
    }
    if (then_ty.is<chunk_ptr>()) {
      diagnostic::error("branches must not return bytes")
        .primary(branch_location(args_.consequence))
        .emit(dh);
      return failure::promise();
    }
    if (args_.alternative and else_ty.is<chunk_ptr>()) {
      diagnostic::error("branches must not return bytes")
        .primary(branch_location(*args_.alternative))
        .emit(dh);
      return failure::promise();
    }
    if (then_ty == else_ty) {
      return then_ty;
    }
    if (then_ty.is<void>()) {
      return else_ty;
    }
    if (else_ty.is<void>()) {
      return then_ty;
    }
    // TODO: Improve diagnostic.
    auto diag = diagnostic::error("incompatible branch output types: {} and {}",
                                  operator_type_name(then_ty),
                                  operator_type_name(else_ty))
                  .primary(branch_location(args_.consequence));
    if (args_.alternative) {
      diag = std::move(diag).secondary(branch_location(*args_.alternative));
    }
    std::move(diag).emit(dh);
    return failure::promise();
  }

  friend auto inspect(auto& f, IfIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  IfArgs args_;
};

// TODO: Clean this up. We might want to be able to just use
// `TENZIR_REGISTER_PLUGINS` also from `libtenzir` itself.
auto register_plugins_somewhat_hackily = std::invoke([]() {
  auto x = std::initializer_list<plugin*>{
    new inspection_plugin<ir::Operator, IfIr>{},
    new inspection_plugin<ir::Operator, ir::SetIr>{},
    make_match_ir_inspection_plugin(),
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

ir::CompileResult::CompileResult(Box<ir::Operator> op) {
  pipeline_.operators.push_back(std::move(op));
}

ir::CompileResult::CompileResult(pipeline pipe) : pipeline_{std::move(pipe)} {
}

auto ir::CompileResult::unwrap() && -> pipeline {
  return std::move(pipeline_);
}

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
              diagnostic::error(
                "This operator is not available in Tenzir Node v6")
                .primary(x.op)
                .hint("{}", get_porting_hint(x.op))
                .hint("see https://tenzir.com/docs/guides/tenzir-v6-migration")
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
            merge_compiled_pipeline(lets, operators,
                                    std::move(compiled).unwrap());
            return {};
          },
          [&](const user_defined_operator& op) -> failure_or<void> {
            ctx.source_map().add_source(op.source);
            auto const callid
              = ctx.source_map().add_call_site(x.op.get_location());
            auto op_name = make_operator_name(x.op);
            auto udo_dh = udo_diagnostic_handler{
              &static_cast<diagnostic_handler&>(ctx), op_name, op};
            // Bind argument expressions in the outer ctx so that any
            // `$outer_let` references are resolved here.
            for (auto& arg : x.args) {
              if (auto* assignment = try_as<ast::assignment>(arg)) {
                TRY(assignment->right.bind(ctx));
              } else {
                TRY(arg.bind(ctx));
              }
            }
            // Validate args and substitute them into the body AST. The
            // session adopts `udo_dh` so that diagnostics also carry the
            // call-site usage and parameters.
            auto sp = session_provider::make(udo_dh);
            auto inv
              = operator_factory_invocation{std::move(x.op), std::move(x.args)};
            TRY(auto substituted, instantiate_user_defined_operator(
                                    op, inv, sp.as_session(), callid, udo_dh));
            // The body is hygienic: it cannot see outer `let` bindings. Any
            // outer references reach the body only through arguments, which
            // we pre-bound above before substitution copied them in.
            auto udo_ctx = ctx.without_env();
            TRY(auto pipe, std::move(substituted).compile(udo_ctx));
            merge_compiled_pipeline(lets, operators, std::move(pipe));
            return {};
          });
      },
      [&](ast::assignment x) -> failure_or<void> {
        TRY(x.left.bind(ctx));
        TRY(resolve_assignment_left(x, ctx));
        TRY(x.right.bind(ctx));
        operators.push_back(make_set_ir(std::move(x)));
        return {};
      },
      [&](ast::let_stmt x) -> failure_or<void> {
        if (try_as<ast::lambda_expr>(*x.expr.kind)) {
          diagnostic::error("lambda-valued `let` bindings are not supported")
            .primary(x.expr)
            .hint("inline the lambda expression at the use site")
            .emit(ctx);
          return failure::promise();
        }
        TRY(x.expr.bind(ctx));
        auto id = scope.let(std::string{x.name_without_dollar()});
        lets.emplace_back(std::move(x.name), std::move(x.expr), id);
        return {};
      },
      [&](ast::if_stmt x) -> failure_or<void> {
        TRY(x.condition.bind(ctx));
        TRY(auto then, std::move(x.then).compile(ctx));
        auto args = IfArgs{};
        args.condition = std::move(x.condition);
        args.consequence = std::move(then);
        if (x.else_) {
          TRY(auto pipe, std::move(x.else_->pipe).compile(ctx));
          args.alternative.emplace(std::move(pipe));
        }
        operators.emplace_back(IfIr{std::move(args)});
        return {};
      },
      [&](ast::match_stmt x) -> failure_or<void> {
        TRY(auto op, make_match_ir(std::move(x), ctx));
        operators.push_back(std::move(op));
        return {};
      },
      [&](ast::type_stmt x) -> failure_or<void> {
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
      if (try_as<ast::lambda_expr>(*let.expr.kind)) {
        diagnostic::error("lambda-valued `let` bindings are not supported")
          .primary(let.expr)
          .hint("inline the lambda expression at the use site")
          .emit(ctx);
        return failure::promise();
      }
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

namespace {

/// Derive the channel kind between two adjacent planned operators.
auto derive_channel_kind(const ir::PlannedOperator& up,
                         const ir::PlannedOperator& down) -> ir::ChannelKind {
  TENZIR_ASSERT(not up.output.is<void>(), "cannot construct a void channel");
  if (up.output.is<chunk_ptr>()) {
    return ir::ChannelKind::Bytes;
  }
  if (not down.partition_keys.empty() and down.parallelism > 1) {
    return ir::ChannelKind::Shuffle;
  }
  if (up.parallelism == down.parallelism) {
    return up.parallelism == 1 ? ir::ChannelKind::Direct
                               : ir::ChannelKind::DirectFused;
  }
  if (up.parallelism == 1) {
    return ir::ChannelKind::Scatter;
  }
  if (down.parallelism == 1) {
    return ir::ChannelKind::Gather;
  }
  TENZIR_TODO(); // this can only happen for N:M parallelism
}

/// Collects the plan's sinks
auto find_sinks(ir::Plan const& plan) -> std::vector<size_t> {
  auto has_out = std::vector<bool>(plan.operators.size(), false);
  for (const auto& channel : plan.channels) {
    for (auto from : channel.from) {
      if (from < plan.operators.size()) {
        has_out[from] = true;
      }
    }
  }
  auto sinks = std::vector<size_t>{};
  for (auto node = size_t{0}; node < plan.operators.size(); ++node) {
    if (has_out[node] or plan.operators[node].output.is_not<void>()) {
      continue;
    }
    sinks.push_back(node);
  }
  return sinks;
}

} // namespace

auto ir::Plan::from(pipeline pipe, element_type_tag input,
                    diagnostic_handler& dh) -> failure_or<Plan> {
  // optimize
  TENZIR_ASSERT(pipe.lets.empty());
  auto opt = std::move(pipe).optimize(optimize_filter{}, event_order::ordered);
  TENZIR_ASSERT(opt.replacement.lets.empty());
  pipe = std::move(opt.replacement);
  for (auto& expr : opt.filter) {
    pipe.operators.insert(pipe.operators.begin(), make_where_ir(expr));
  }
  // construct plan
  auto plan = Plan{};
  plan.operators.reserve(pipe.operators.size());
  auto builder = PlanBuilder{plan};
  auto head = PlanPorts{PlanPort{.node = PlanPort::input, .type = input}};
  TRY(auto tail, builder.lower_pipeline(std::move(pipe), std::move(head), dh));
  // bundle tail and sinks (to gather all output signals)
  auto sinks = find_sinks(plan);
  if (not tail.empty()) {
    sinks.insert(sinks.begin(), builder.into_single(tail).node);
  }
  auto kind
    = sinks.size() > 1 ? ChannelKind::GatherSignals : ChannelKind::Direct;
  plan.channels.push_back(PlannedChannel{
    .from = std::move(sinks),
    .to = {PlanPort::output},
    .kind = kind,
  });
  return plan;
}

namespace {

auto find_external_input_channel(ir::Plan const& plan)
  -> ir::PlannedChannel const& {
  auto* result = static_cast<ir::PlannedChannel const*>(nullptr);
  for (auto const& channel : plan.channels) {
    if (channel.from == std::vector<size_t>{ir::PlanPort::input}) {
      TENZIR_ASSERT(result == nullptr);
      result = &channel;
    }
  }
  TENZIR_ASSERT(result != nullptr);
  return *result;
}

auto find_external_output_channel(ir::Plan const& plan)
  -> ir::PlannedChannel const& {
  auto* result = static_cast<ir::PlannedChannel const*>(nullptr);
  for (auto const& channel : plan.channels) {
    if (channel.to == std::vector<size_t>{ir::PlanPort::output}) {
      TENZIR_ASSERT(result == nullptr);
      result = &channel;
    }
  }
  TENZIR_ASSERT(result != nullptr);
  return *result;
}

} // namespace

auto ir::Plan::input_type() const -> element_type_tag {
  auto const& channel = find_external_input_channel(*this);
  TENZIR_ASSERT(channel.to.size() == 1);
  return operators[channel.to.front()].input;
}

auto ir::Plan::output_type() const -> element_type_tag {
  auto const& channel = find_external_output_channel(*this);
  TENZIR_ASSERT(not channel.from.empty());
  return operators[channel.from.front()].output;
}

auto ir::Operator::plan(PlanBuilder& builder, PlanPorts input,
                        diagnostic_handler& dh) && -> failure_or<PlanPorts> {
  TENZIR_ASSERT(not input.empty());
  auto in = input.front().type;
  TRY(auto out_ty, infer_type(in, dh));
  auto node = builder.add_node(std::move(*this).move(), in, out_ty);
  builder.add_channel(input, node);
  return out_ty.is<void>() ? PlanPorts{} : PlanPorts{PlanPort{node, out_ty}};
}

auto ir::PlanBuilder::add_node(Box<Operator> op, element_type_tag input,
                               element_type_tag output) -> size_t {
  // Query parallelizability and partition keys before moving the operator. The
  // planner picks the exact degree of parallelism for replicable operators.
  auto parallelism
    = op->parallelizable() ? size_t{std::thread::hardware_concurrency()} : 1;
  auto partition_keys = op->partition_keys();
  auto node = plan_.operators.size();
  plan_.operators.push_back(PlannedOperator{
    .op = std::move(op),
    .parallelism = parallelism,
    .partition_keys = std::move(partition_keys),
    .input = input,
    .output = output,
  });
  return node;
}

auto ir::PlanBuilder::add_channel(std::vector<size_t> from,
                                  std::vector<size_t> to, ChannelKind kind)
  -> void {
  plan_.channels.push_back(PlannedChannel{
    .from = std::move(from),
    .to = std::move(to),
    .kind = kind,
  });
}

auto ir::PlanBuilder::add_channel(const PlanPorts& from, size_t to) -> void {
  TENZIR_ASSERT(not from.empty());
  if (from[0].node == PlanPort::input) {
    auto to_op = plan_.operators[to];
    if (to_op.parallelism > 1 or not to_op.partition_keys.empty()) {
      // When {input} should be scattered/shuffled, we have to inject an
      // identity operator, because the input channel must `Direct`.
      auto identity = add_identity(to_op.input);
      add_channel({from[0].node}, {identity}, ChannelKind::Direct);
      auto kind
        = derive_channel_kind(plan_.operators[identity], plan_.operators[to]);
      add_channel({identity}, {to}, kind);
      return;
    }
    add_channel({from[0].node}, {to}, ChannelKind::Direct);
    return;
  }
  if (to == PlanPort::output) {
    TENZIR_ASSERT_EQ(from.size(), 1);
    add_channel({from[0].node}, {to}, ChannelKind::Direct);
    return;
  }
  if (from.size() > 1) {
    // gather
    auto froms = std::vector<size_t>{};
    froms.reserve(from.size());
    for (const auto& port : from) {
      froms.push_back(port.node);
    }
    add_channel(std::move(froms), {to}, ChannelKind::Gather);
    return;
  }
  auto kind
    = derive_channel_kind(plan_.operators[from[0].node], plan_.operators[to]);
  add_channel({from[0].node}, {to}, kind);
}

auto ir::PlanBuilder::add_broadcast(PlanPort from, std::vector<size_t> to)
  -> void {
  TENZIR_ASSERT(from.node != PlanPort::input);
  add_channel({from.node}, std::move(to), ChannelKind::Broadcast);
}

namespace {

/// Runtime pass-through used to materialize identity IR nodes.
template <class T>
class PassOp final : public Operator<T, T> {
public:
  auto process(T input, Push<T>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

template <>
class PassOp<void> final : public Operator<void, void> {
public:
  auto state() -> OperatorState override {
    return OperatorState::done;
  }
};

/// A stateless identity IR operator that forwards its input unchanged. The
/// planner inserts it to give branches a single entry node and to merge
/// fan-out frontiers; it never appears in a serialized `ir::pipeline`.
class IdentityIr final : public ir::Operator {
public:
  auto name() const -> std::string override {
    return "pass";
  }

  auto copy() const -> Box<ir::Operator> override {
    return IdentityIr{};
  }

  auto move() && -> Box<ir::Operator> override {
    return IdentityIr{};
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler&) const
    -> failure_or<element_type_tag> override {
    return input;
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    return match(input, []<class T>(tag<T>) -> AnyOperator {
      return Box<tenzir::Operator<T, T>>{PassOp<T>{}.with_name("pass")};
    });
  }
};

auto make_identity_ir() -> Box<ir::Operator> {
  return IdentityIr{};
}

} // namespace

auto ir::PlanBuilder::into_single(const PlanPorts& from) -> PlanPort {
  TENZIR_ASSERT(not from.empty());
  if (from.size() == 1 and from.front().node != PlanPort::input) {
    auto const& up = plan_.operators[from.front().node];
    // A single parallelized port still has `parallelism` runtime instances,
    // so collapse it through an identity to expose a proper single-instance
    // output to the downstream boundary (e.g. the plan's `{output}`).
    if (up.parallelism <= 1) {
      return from.front();
    }
  }
  // Collapse the frontier (a lone external source, or multiple upstreams) into
  // a single node by routing it through an identity operator.
  auto type = from.front().type;
  auto node = add_node(make_identity_ir(), type, type);
  add_channel(from, node);
  return PlanPort{node, type};
}

auto ir::PlanBuilder::add_identity(element_type_tag type) -> size_t {
  return add_node(make_identity_ir(), type, type);
}

auto ir::PlanBuilder::lower_pipeline(pipeline pipe, PlanPorts input,
                                     diagnostic_handler& dh)
  -> failure_or<PlanPorts> {
  TENZIR_ASSERT(pipe.lets.empty());
  PlanPorts frontier = std::move(input);
  for (auto& op : pipe.operators) {
    TRY(frontier, std::move(*op).plan(*this, std::move(frontier), dh));
  }
  return frontier;
}

auto ir::pipeline::infer_type(element_type_tag input,
                              diagnostic_handler& dh) const
  -> failure_or<element_type_tag> {
  auto frontier = input;
  for (auto& op : operators) {
    TRY(frontier, op->infer_type(frontier, dh));
  }
  return frontier;
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
  TENZIR_UNUSED(order);
  auto replacement = std::vector<Box<Operator>>{};
  replacement.push_back(std::move(*this).move());
  for (auto& expr : filter) {
    replacement.push_back(make_where_ir(std::move(expr)));
  }
  return {
    optimize_filter{},
    event_order::ordered,
    pipeline{{}, std::move(replacement)},
  };
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
  return Box<Operator>::from_non_null(std::move(copy));
}

auto ir::Operator::move() && -> Box<Operator> {
  // TODO: This should be overriden by something like CRTP.
  return copy();
}

auto ir::Operator::display_name() const -> std::string {
  auto n = name();
  if (n.ends_with("_ir")) {
    n.resize(n.length() - 3);
  }
  return n;
}

auto ir::Operator::infer_type(element_type_tag input, diagnostic_handler&) const
  -> failure_or<element_type_tag> {
  return input;
}

auto operator_compiler_plugin::operator_name() const -> std::string {
  auto result = name();
  if (result.starts_with("tql2.")) {
    result = result.substr(5);
  }
  return result;
}

ir::pipeline::pipeline(std::vector<let> lets,
                       std::vector<Box<Operator>> operators)
  : lets{std::move(lets)}, operators{std::move(operators)} {
}

} // namespace tenzir
