//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"

#include "tenzir/async.hpp"
#include "tenzir/base_ctx.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/ir_if.hpp"
#include "tenzir/ir_match.hpp"
#include "tenzir/ir_set.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/tql2/user_defined_operator.hpp"

#include <algorithm>
#include <ranges>
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

} // namespace

auto combine_branch_types(std::optional<element_type_tag> lhs,
                          std::optional<element_type_tag> rhs, location primary,
                          diagnostic_handler& dh)
  -> failure_or<std::optional<element_type_tag>> {
  if (not lhs) {
    return rhs;
  }
  if (not rhs) {
    return lhs;
  }
  if (*lhs == *rhs) {
    return lhs;
  }
  if (lhs->is<void>()) {
    return rhs;
  }
  if (rhs->is<void>()) {
    return lhs;
  }
  diagnostic::error("incompatible branch output types: {} and {}",
                    operator_type_name(*lhs), operator_type_name(*rhs))
    .primary(primary)
    .emit(dh);
  return failure::promise();
}

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
  auto acc = ir::pipeline{};
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
            acc.append(std::move(compiled).unwrap());
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
            acc.append(std::move(pipe));
            return {};
          });
      },
      [&](ast::assignment x) -> failure_or<void> {
        TRY(x.left.bind(ctx));
        TRY(resolve_assignment_left(x, ctx));
        TRY(x.right.bind(ctx));
        acc.operators.push_back(make_set_ir(std::move(x)));
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
        acc.lets.emplace_back(std::move(x.name), std::move(x.expr), id);
        return {};
      },
      [&](ast::if_stmt x) -> failure_or<void> {
        TRY(auto op, make_if_ir(std::move(x), ctx));
        acc.operators.push_back(std::move(op));
        return {};
      },
      [&](ast::match_stmt x) -> failure_or<void> {
        TRY(auto op, make_match_ir(std::move(x), ctx));
        acc.operators.push_back(std::move(op));
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
  return acc;
}

auto ir::pipeline::bind(let_id id, ast::constant::kind value) -> void {
  // Prepend so the binding is in scope for all subsequent `let`s and operators,
  // matching the semantics of a base-environment binding.
  auto value_ex
    = ast::expression{ast::constant{std::move(value), location::unknown}};
  lets.insert(lets.begin(), let{ast::identifier{}, value_ex, id});
}

auto ir::pipeline::prepend(pipeline other) -> void {
  lets.insert(lets.begin(), std::move_iterator{other.lets.begin()},
              std::move_iterator{other.lets.end()});
  operators.insert(operators.begin(),
                   std::move_iterator{other.operators.begin()},
                   std::move_iterator{other.operators.end()});
}

auto ir::pipeline::prepend(optimize_filter filter) -> void {
  operators.insert_range(operators.begin(),
                         filter | std::views::as_rvalue
                           | std::views::transform(make_where_ir));
}

auto ir::pipeline::append(pipeline other) -> void {
  lets.insert(lets.end(), std::move_iterator{other.lets.begin()},
              std::move_iterator{other.lets.end()});
  operators.insert(operators.end(), std::move_iterator{other.operators.begin()},
                   std::move_iterator{other.operators.end()});
}

auto ir::pipeline::append(optimize_filter filter) -> void {
  operators.insert_range(operators.end(),
                         filter | std::views::as_rvalue
                           | std::views::transform(make_where_ir));
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
      auto constant = ast::constant::make(value);
      auto inserted = env.try_emplace(let.id, std::move(constant)).second;
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

auto ir::make_plan(pipeline pipe, element_type_tag input, base_ctx ctx)
  -> failure_or<Plan> {
  // Resolve `let` bindings and substitute non-deterministic arguments. This is
  // the single substitution point for all pipelines, including subpipelines
  // that inject runtime values via `pipeline::bind`.
  TRY(pipe.substitute(substitute_ctx{ctx, nullptr}, true));
  TENZIR_ASSERT(pipe.lets.empty());
  // Optimize the now-instantiated pipeline. Any filter left over after
  // optimization is reinserted as leading `where` operators.
  auto opt = std::move(pipe).optimize(optimize_filter{}, event_order::ordered);
  TENZIR_ASSERT(opt.replacement.lets.empty());
  pipe = std::move(opt.replacement);
  // Prepend the leftover filters as leading `where` operators.
  pipe.prepend(std::move(opt.filter));
  // Type-check the instantiated pipeline against `input`. Performing this here
  // means spawning the resulting `Plan` can no longer fail.
  TRY(auto output, pipe.infer_type(input, ctx));
  return Plan{std::move(pipe), input, output};
}

auto ir::Plan::spawn() && -> std::vector<AnyOperator> {
  return std::move(pipe_).spawn(input_);
}

auto ir::pipeline::spawn(element_type_tag input) && -> std::vector<AnyOperator> {
  // The caller is responsible for instantiating and optimizing the
  // pipeline via `ir::make_plan` before spawning, so there must be no
  // remaining `let` bindings here.
  TENZIR_ASSERT(lets.empty());
  auto result = std::vector<AnyOperator>{};
  for (auto& op : operators) {
    // We already checked, there should be no diagnostics here.
    auto dh = null_diagnostic_handler{};
    auto output = op->infer_type(input, dh);
    TENZIR_ASSERT(output);
    result.push_back(op->spawn(input));
    input = *output;
  }
  return result;
}

auto ir::pipeline::infer_type(element_type_tag input,
                              diagnostic_handler& dh) const
  -> failure_or<element_type_tag> {
  for (auto& op : operators) {
    TRY(input, op->infer_type(input, dh));
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
