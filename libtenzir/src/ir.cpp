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
#include "tenzir/detail/narrow.hpp"
#include "tenzir/ir_if.hpp"
#include "tenzir/ir_match.hpp"
#include "tenzir/ir_set.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/tql2/user_defined_operator.hpp"
#include "tenzir/type.hpp"

#include <algorithm>
#include <ranges>
#include <span>
#include <utility>

namespace tenzir {

auto ir::split_conjunction(ast::expression predicate) -> OptimizeFilter {
  auto result = OptimizeFilter{};
  // Visits the operands from left to right, which keeps their order.
  auto split = [&](this auto const& self, ast::expression expr) -> void {
    if (auto* binary = try_as<ast::binary_expr>(expr);
        binary and binary->op == ast::binary_op::and_) {
      self(std::move(binary->left));
      self(std::move(binary->right));
      return;
    }
    result.push_back(std::move(expr));
  };
  split(std::move(predicate));
  return result;
}

auto ir::split_filter_by_dependents(ir::OptimizeFilter filter,
                                    const ast::ExprRefs& touched)
  -> ir::split_filter_result {
  auto result = ir::split_filter_result{};
  if (touched.let_ids.empty() and touched.field_paths.empty()) {
    result.independent = std::move(filter);
    return result;
  }
  for (auto& expr : filter) {
    auto refs = ast::collect_refs(expr);
    // A predicate behind one that stays must not move past it if it could fail
    // for the events that the other one drops.
    if (not refs or refs->overlaps(touched)
        or (not result.dependent.empty() and not is_infallible(expr))) {
      result.dependent.push_back(std::move(expr));
    } else {
      result.independent.push_back(std::move(expr));
    }
  }
  return result;
}

auto ir::split_filter_by_substitution(OptimizeFilter filter,
                                      registry const& reg,
                                      ast::FieldSubstitution const& fields,
                                      ast::MetaSubstitution const& meta)
  -> split_filter_result {
  auto result = split_filter_result{};
  for (auto& expr : filter) {
    auto substituted = ast::substitute_refs(expr, reg, fields, meta);
    if (not substituted) {
      result.dependent.push_back(std::move(expr));
      continue;
    }
    auto simplified = simplify_predicate(std::move(*substituted), reg);
    // A predicate that grows past the bound stays behind the operator.
    auto const size = expression_size(simplified);
    if (size > max_rewritten_predicate_size and size > expression_size(expr)) {
      result.dependent.push_back(std::move(expr));
      continue;
    }
    // As in `split_filter_by_dependents`, a predicate must not move past one
    // that stays if it could fail for the events that the other one drops.
    if (not result.dependent.empty() and not is_infallible(simplified)) {
      result.dependent.push_back(std::move(expr));
      continue;
    }
    result.independent.push_back(std::move(simplified));
  }
  return result;
}

auto ir::rewrite_aggregation(OptimizeAggregation aggregation,
                             ast::FieldSubstitution const& fields)
  -> Option<OptimizeAggregation> {
  for (auto& column : aggregation.columns) {
    if (not column.input) {
      continue;
    }
    auto const& input = *column.input;
    TENZIR_ASSERT(not input.empty());
    auto expr = ast::expression{ast::root_field{
      ast::identifier{input.front(), location::unknown},
      false,
    }};
    for (auto const& name : std::span{input}.subspan(1)) {
      expr = ast::expression{ast::field_access{
        std::move(expr),
        location::unknown,
        false,
        ast::identifier{name, location::unknown},
      }};
    }
    auto field = ast::field_path::try_from(std::move(expr));
    TENZIR_ASSERT(field);
    TRY(auto resolved, fields(*field));
    TRY(auto path, ast::field_path::try_from(std::move(resolved)));
    if (path.has_this() or path.path().empty()) {
      return None{};
    }
    auto segments = std::vector<std::string>{};
    segments.reserve(path.path().size());
    for (auto const& segment : path.path()) {
      if (segment.has_question_mark) {
        return None{};
      }
      segments.push_back(segment.id.name);
    }
    column.input = std::move(segments);
  }
  return aggregation;
}

auto ir::is_field_path_prefix(const ast::field_path& prefix,
                              const ast::field_path& path) -> bool {
  const auto lhs = prefix.path();
  const auto rhs = path.path();
  if (lhs.size() > rhs.size()) {
    return false;
  }
  const auto name = [](const ast::field_path::segment& segment) {
    return std::string_view{segment.id.name};
  };
  return std::ranges::equal(lhs, rhs.first(lhs.size()), {}, name, name);
}

auto ir::add_to_projection(Option<OptimizeProjection>& projection,
                           const ast::field_path& path) -> void {
  if (not projection) {
    return;
  }
  if (path.path().empty()) {
    projection = None{};
    return;
  }
  const auto equal = [&](const ast::field_path& other) {
    return other.path().size() == path.path().size()
           and is_field_path_prefix(other, path);
  };
  if (std::ranges::none_of(*projection, equal)) {
    projection->push_back(path);
  }
}

auto ir::add_refs_to_projection(Option<OptimizeProjection>& projection,
                                const ast::expression& expr) -> void {
  if (not projection) {
    return;
  }
  auto refs = ast::collect_refs(expr);
  if (not refs) {
    projection = None{};
    return;
  }
  for (const auto& path : refs->field_paths) {
    add_to_projection(projection, path);
    if (not projection) {
      return;
    }
  }
}

auto ir::merge_projection(Option<OptimizeProjection>& projection,
                          const Option<OptimizeProjection>& other) -> void {
  if (not projection) {
    return;
  }
  if (not other) {
    projection = None{};
    return;
  }
  for (const auto& path : *other) {
    add_to_projection(projection, path);
  }
}

auto ir::intersect_projection(Option<OptimizeProjection>& projection,
                              const Option<OptimizeProjection>& other) -> void {
  if (not other) {
    return;
  }
  if (not projection) {
    projection = other;
    return;
  }
  auto result = Option<OptimizeProjection>{OptimizeProjection{}};
  for (auto const& lhs : *projection) {
    for (auto const& rhs : *other) {
      if (is_field_path_prefix(lhs, rhs)) {
        add_to_projection(result, rhs);
      } else if (is_field_path_prefix(rhs, lhs)) {
        add_to_projection(result, lhs);
      }
    }
  }
  projection = std::move(result);
}

auto make_where_ir(ast::expression filter, registry const& reg)
  -> Box<ir::Operator> {
  // TODO: This should just be a `where_ir{std::move(filter)}`.
  const auto* where = plugins::find<operator_compiler_plugin>("where");
  TENZIR_ASSERT(where);
  auto args = std::vector<ast::expression>{};
  args.push_back(std::move(filter));
  // TODO: This is a terrible workaround. We are discarding diagnostics and
  // creating a new compile context, which should be created only once.
  auto dh = null_diagnostic_handler{};
  auto ctx = compile_ctx::make_root(base_ctx{dh, reg});
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

ir::CompileResult::CompileResult(Box<ir::Operator> op) {
  pipeline_.operators.push_back(std::move(op));
}

ir::CompileResult::CompileResult(pipeline pipe) : pipeline_{std::move(pipe)} {
}

auto ir::CompileResult::unwrap() && -> pipeline {
  return std::move(pipeline_);
}

auto ast::pipeline::compile(compile_ctx ctx) && -> failure_or<ir::pipeline> {
  // Only whoever has the code knows what it may name, so it resolves the code
  // before compiling it.
  TENZIR_ASSERT(entities_resolved(*this),
                "a pipeline has to be resolved before it is compiled");
  auto acc = ir::pipeline{};
  acc.definition_location = get_location();
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
            auto sp = session_provider::make(udo_dh, ctx.reg());
            auto inv
              = operator_factory_invocation{std::move(x.op), std::move(x.args)};
            TRY(auto substituted, instantiate_user_defined_operator(
                                    op, inv, sp.as_session(), callid, udo_dh));
            // The body is hygienic: it cannot see outer `let` bindings. Any
            // outer references reach the body only through arguments, which
            // we pre-bound above before substitution copied them in.
            auto udo_ctx = ctx.without_env();
            // A package's body is resolved when the package is added if it
            // declares what it uses, which leaves nothing to do here. One
            // that does not is resolved here, against every package, which
            // is deprecated.
            TRY(resolve_entities(
              substituted, ExternPrelude::all_packages_deprecated(), udo_ctx));
            TRY(auto pipe, std::move(substituted).compile(udo_ctx));
            acc.append(std::move(pipe));
            return {};
          });
      },
      [&](ast::assignment x) -> failure_or<void> {
        TRY(x.left.bind(ctx));
        TRY(validate_assignment_target(x.left, ctx));
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

auto ir::pipeline::prepend(OptimizeFilter filter, registry const& reg) -> void {
  operators.insert_range(operators.begin(),
                         filter | std::views::as_rvalue
                           | std::views::transform([&](ast::expression expr) {
                               return make_where_ir(std::move(expr), reg);
                             }));
}

auto ir::pipeline::append(pipeline other) -> void {
  lets.insert(lets.end(), std::move_iterator{other.lets.begin()},
              std::move_iterator{other.lets.end()});
  operators.insert(operators.end(), std::move_iterator{other.operators.begin()},
                   std::move_iterator{other.operators.end()});
}

auto ir::pipeline::append(OptimizeFilter filter, registry const& reg) -> void {
  operators.insert_range(operators.end(),
                         filter | std::views::as_rvalue
                           | std::views::transform([&](ast::expression expr) {
                               return make_where_ir(std::move(expr), reg);
                             }));
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
      auto value = ast::expression{};
      // Only the secret lookups wait for operator startup. Everything else is
      // evaluated once, here, so that every reference inlines the same
      // deterministic recipe.
      TRY(auto binding, nova::fold_startup_binding(let.expr, ctx));
      if (binding.requires_startup) {
        if (not binding.deterministic) {
          diagnostic::error(
            "`let` binding that uses `secret` must be deterministic")
            .primary(let.expr)
            .note("non-deterministic functions cannot depend on a secret")
            .emit(ctx);
          return failure::promise();
        }
        value = std::move(let.expr);
      } else {
        TRY(auto constant, const_eval(let.expr, ctx));
        value = ast::constant::make(std::move(constant));
      }
      auto inserted = env.try_emplace(let.id, std::move(value)).second;
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

auto ir::pipeline::infer_type(element_type_tag input, base_ctx ctx) const
  -> failure_or<element_type_tag> {
  auto frontier = input;
  for (auto& op : operators) {
    TRY(frontier, op->infer_type(frontier, ctx));
  }
  return frontier;
}

auto ir::pipeline::optimize(OptimizeRequest req,
                            const OptimizeCtx& octx) && -> OptimizeResult {
  auto replacement = pipeline{std::move(lets), {}};
  replacement.definition_location = definition_location;
  for (auto& op : std::ranges::reverse_view(operators)) {
    // An operator that runs across multiple parallel instances has its output
    // reordered by the gather that follows it, so its consumer cannot rely on
    // its order anyway.
    if (octx.can_any_op_reorder and op->max_parallelism() > 1) {
      req.order = EventOrder::unordered;
    }
    // Operators see and forward only simplified predicates.
    req.filter = simplify_filter(std::move(req.filter), octx.reg);
    auto opt = std::move(*op).optimize(std::move(req), octx);
    req = OptimizeRequest{
      .filter = std::move(opt.filter),
      .order = opt.order,
      .limit = opt.limit,
      .projection = std::move(opt.projection),
    };
    replacement.operators.insert(
      replacement.operators.begin(),
      std::move_iterator{opt.replacement.operators.begin()},
      std::move_iterator{opt.replacement.operators.end()});
  }
  // Let an operator compute an aggregation that follows it, either directly
  // or behind operators that can rewrite the aggregation over their input,
  // such as renames. This runs after the walk, when every operator has
  // absorbed what it can, so that predicates kept between the operators
  // prevent the fusion. The fallback replays the skipped operators.
  auto& ops = replacement.operators;
  for (auto j = size_t{1}; j < ops.size(); ++j) {
    auto aggregation = ops[j]->aggregation();
    for (auto k = j; aggregation and k > 0;) {
      --k;
      if (ops[k]->absorb_aggregation(*aggregation)) {
        ops.erase(ops.begin() + detail::narrow<std::ptrdiff_t>(k + 1),
                  ops.begin() + detail::narrow<std::ptrdiff_t>(j + 1));
        j = k;
        break;
      }
      aggregation = ops[k]->rewrite_aggregation(std::move(*aggregation), octx);
      if (aggregation) {
        auto& fallback = aggregation->fallback.operators;
        fallback.insert(fallback.begin(), ops[k]->copy());
      }
    }
  }
  return {
    .filter = simplify_filter(std::move(req.filter), octx.reg),
    .order = req.order,
    .replacement = std::move(replacement),
    .limit = req.limit,
    .projection = std::move(req.projection),
  };
}

auto ir::Operator::optimize(OptimizeRequest req,
                            const OptimizeCtx& octx) && -> OptimizeResult {
  auto replacement = std::vector<Box<Operator>>{};
  replacement.push_back(std::move(*this).move());
  for (auto& expr : req.filter) {
    replacement.push_back(make_where_ir(std::move(expr), octx.reg));
  }
  return {
    OptimizeFilter{},
    EventOrder::ordered,
    pipeline{{}, std::move(replacement)},
  };
}

auto ir::Operator::aggregation() const -> Option<OptimizeAggregation> {
  return None{};
}

auto ir::Operator::absorb_aggregation(OptimizeAggregation const& aggregation)
  -> bool {
  TENZIR_UNUSED(aggregation);
  return false;
}

auto ir::Operator::rewrite_aggregation(OptimizeAggregation aggregation,
                                       OptimizeCtx const& octx) const
  -> Option<OptimizeAggregation> {
  TENZIR_UNUSED(aggregation, octx);
  return None{};
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

auto ir::Operator::infer_type(element_type_tag input, base_ctx) const
  -> failure_or<element_type_tag> {
  return input;
}

auto operator_compiler_plugin::operator_name() const -> std::string {
  return name();
}

ir::pipeline::pipeline(std::vector<let> lets,
                       std::vector<Box<Operator>> operators)
  : lets{std::move(lets)}, operators{std::move(operators)} {
}

} // namespace tenzir
