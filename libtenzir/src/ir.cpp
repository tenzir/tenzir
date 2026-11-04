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

#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <map>
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

auto ir::split_filter(
  ir::OptimizeFilter filter,
  std::function<auto(ast::expression const&)->bool> const& movable)
  -> ir::split_filter_result {
  auto result = ir::split_filter_result{};
  for (auto& expr : filter) {
    // A predicate behind one that stays must not move past it if it could fail
    // for the events that the other one drops.
    if (not movable(expr)
        or (not result.dependent.empty() and not is_infallible(expr))) {
      result.dependent.push_back(std::move(expr));
    } else {
      result.independent.push_back(std::move(expr));
    }
  }
  return result;
}

auto ir::split_filter_by_dependents(ir::OptimizeFilter filter,
                                    const ast::ExprRefs& touched)
  -> ir::split_filter_result {
  if (touched.let_ids.empty() and touched.field_paths.empty()) {
    return {.independent = std::move(filter), .dependent = {}};
  }
  return split_filter(std::move(filter), [&](ast::expression const& expr) {
    auto refs = ast::collect_refs(expr);
    return refs and not refs->overlaps(touched);
  });
}

namespace {

/// Identifies the value that a replacement computes. Replacements for
/// different members of the same assigned value compute the same value, so the
/// members do not count, unless `members` is set.
auto computation_key(ast::expression const& replacement, bool members = false)
  -> caf::byte_buffer {
  auto const* base = &replacement;
  while (not members) {
    if (auto const* access = try_as<ast::field_access>(*base)) {
      base = &access->left;
    } else if (auto const* index = try_as<ast::index_expr>(*base)) {
      base = &index->expr;
    } else {
      break;
    }
  }
  auto buffer = caf::byte_buffer{};
  auto f = caf::binary_serializer{buffer};
  auto copy = *base;
  auto const success = f.apply(copy);
  TENZIR_ASSERT(success);
  return buffer;
}

} // namespace

auto ir::split_filter_by_substitution(OptimizeFilter filter,
                                      registry const& reg,
                                      ast::FieldSubstitution const& fields,
                                      ast::MetaSubstitution const& meta)
  -> split_filter_result {
  // A replacement that is not plain stands for a value that the operator
  // computes, which a moved predicate computes once more. That is acceptable
  // for a cheap value that the moved predicates read once in total, but
  // copying it several times multiplies the work. A predicate that ends up
  // with more expensive parts, such as parsers, than it had may repeat
  // arbitrarily expensive work. The simplifier may describe an expensive call
  // by its arguments, such as a field of `ocsf_cast(this)` by the input, in
  // which case the predicate moves if it reads that field once.
  auto result = split_filter_result{};
  // The reads of computed values by the predicates that moved.
  auto reads = std::map<caf::byte_buffer, size_t>{};
  for (auto& expr : filter) {
    auto computed = std::map<caf::byte_buffer, size_t>{};
    auto const counting = ast::FieldSubstitution{
      [&](ast::field_path const& field) -> Option<ast::expression> {
        auto replacement = fields(field);
        if (replacement and not is_plain(*replacement)) {
          // The simplifier may describe a member of an expensive value by a
          // cheap computation of only that member, such as `ocsf_cast(this).a`
          // by `_ocsf_cast_field(…)`, so each member counts on its own.
          auto const cheap = is_cheap(*replacement, reg);
          ++computed[computation_key(*replacement, not cheap)];
        }
        return replacement;
      }};
    auto substituted = ast::substitute_refs(expr, reg, counting, meta);
    if (not substituted) {
      result.dependent.push_back(std::move(expr));
      continue;
    }
    auto predicate = simplify_predicate(std::move(*substituted), reg);
    auto const copied = std::ranges::any_of(computed, [&](auto const& entry) {
      auto const it = reads.find(entry.first);
      return entry.second + (it == reads.end() ? 0 : it->second) > 1;
    });
    if (copied
        or count_expensive(predicate, reg) > count_expensive(expr, reg)) {
      result.dependent.push_back(std::move(expr));
      continue;
    }
    // A predicate that grows past the bound stays behind the operator.
    auto const size = expression_size(predicate);
    if (size > max_rewritten_predicate_size and size > expression_size(expr)) {
      result.dependent.push_back(std::move(expr));
      continue;
    }
    // As in `split_filter_by_dependents`, a predicate must not move past one
    // that stays if it could fail for the events that the other one drops.
    if (not result.dependent.empty() and not is_infallible(predicate)) {
      result.dependent.push_back(std::move(expr));
      continue;
    }
    for (auto const& [key, count] : computed) {
      reads[key] += count;
    }
    result.independent.push_back(std::move(predicate));
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

auto ir::project_assignment(Option<OptimizeProjection>& projection,
                            ast::field_path const& target) -> void {
  if (target.path().empty()) {
    projection = OptimizeProjection{};
    return;
  }
  if (not projection) {
    return;
  }
  std::erase_if(*projection, [&](ast::field_path const& path) {
    return is_field_path_prefix(target, path);
  });
  if (target.path().size() > 1) {
    add_to_projection(projection, target);
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

namespace {

/// Collects the stages of the operators at the front of `ops` for as long as
/// a source can compute them after an aggregation: predicates and at most one
/// sort, optionally followed by a limit, after which nothing follows. A
/// predicate after a sort that keeps only its first events would no longer
/// commute with it, so it ends the stages, too.
auto aggregation_stages(std::span<Box<ir::Operator> const> ops,
                        ir::OptimizeCtx const& octx)
  -> std::vector<ir::QueryStage> {
  auto result = std::vector<ir::QueryStage>{};
  auto sorted = false;
  auto bounded = false;
  for (auto const& op : ops) {
    auto stage = op->query_stage(octx);
    if (not stage) {
      break;
    }
    auto proceed = match(
      stage->kind,
      [&](ir::QueryStage::Filter const&) {
        return not bounded;
      },
      [&](ir::QueryStage::Sort const& sort) {
        if (sorted) {
          return false;
        }
        sorted = true;
        bounded = sort.bound.has_value();
        return true;
      },
      [&](ir::QueryStage::Limit const&) {
        return true;
      },
      [](auto const&) {
        return false;
      });
    if (not proceed) {
      break;
    }
    auto const limit = is<ir::QueryStage::Limit>(stage->kind);
    result.push_back(std::move(*stage));
    if (limit) {
      break;
    }
  }
  return result;
}

/// A stage of a chain, and the index of the last operator that it stands for.
/// It stands for the operators from the one after the previous stage.
struct ChainStage {
  ir::QueryStage stage;
  size_t last;
};

/// Returns an `Aggregate` stage for the operators at the front of `ops`, if
/// they end with an aggregation and each one before it can rewrite it over
/// its input, such as `drop` or renames.
auto fold_aggregation(std::span<Box<ir::Operator> const> ops,
                      ir::OptimizeCtx const& octx) -> Option<ChainStage> {
  for (auto j = size_t{0}; j < ops.size(); ++j) {
    auto aggregation = ops[j]->aggregation();
    if (not aggregation) {
      continue;
    }
    for (auto m = j; aggregation and m > 0; --m) {
      aggregation
        = ops[m - 1]->rewrite_aggregation(std::move(*aggregation), octx);
    }
    if (not aggregation) {
      return None{};
    }
    return ChainStage{
      .stage = {ir::QueryStage::Aggregate{
        .columns = std::move(aggregation->columns),
        .schema_name = std::move(aggregation->schema_name),
      }},
      .last = j,
    };
  }
  return None{};
}

/// Collects the chain of stages behind a source, whose following operators
/// are `ops`. The chain holds at most one aggregation, and a sort that keeps
/// only its first events comes only together with the limit behind it.
auto collect_chain(std::span<Box<ir::Operator> const> ops,
                   ir::OptimizeCtx const& octx)
  -> std::pair<std::vector<ChainStage>, std::string> {
  auto result = std::vector<ChainStage>{};
  auto aggregated = false;
  auto i = size_t{0};
  while (i < ops.size()) {
    auto entry = aggregated ? Option<ChainStage>{}
                            : fold_aggregation(ops.subspan(i), octx);
    if (entry) {
      entry->last += i;
    } else {
      auto stage = ops[i]->query_stage(octx);
      if (not stage) {
        return {std::move(result),
                fmt::format("`{}` is no stage", ops[i]->display_name())};
      }
      entry = ChainStage{.stage = std::move(*stage), .last = i};
    }
    if (auto const* sort = try_as<ir::QueryStage::Sort>(entry->stage.kind);
        sort and sort->bound) {
      auto next = entry->last + 1 < ops.size()
                    ? ops[entry->last + 1]->query_stage(octx)
                    : Option<ir::QueryStage>{};
      if (not next or not is<ir::QueryStage::Limit>(next->kind)) {
        return {std::move(result), "`sort` keeps only its first events, but "
                                   "no limit follows it"};
      }
    }
    if (is<ir::QueryStage::Aggregate>(entry->stage.kind)) {
      if (aggregated) {
        return {std::move(result), "a second aggregation"};
      }
      aggregated = true;
    }
    i = entry->last + 1;
    result.push_back(std::move(*entry));
  }
  return {std::move(result), std::string{}};
}

/// Offers `aggregation` to `op` with the longest prefix of `stages` first,
/// whose operators are `stage_ops`, and returns the number of stages that
/// `op` took over, or `None` if it declined. The fallback of each candidate
/// replays the operators of its stages after the aggregation.
auto absorb_with_stages(ir::Operator& op,
                        ir::OptimizeAggregation const& aggregation,
                        std::span<ir::QueryStage const> stages,
                        std::span<Box<ir::Operator> const> stage_ops)
  -> Option<size_t> {
  TENZIR_ASSERT(stage_ops.size() >= stages.size());
  for (auto n = stages.size() + 1; n > 0; --n) {
    auto const count = n - 1;
    auto candidate = aggregation;
    candidate.stages.assign(
      stages.begin(), stages.begin() + detail::narrow<std::ptrdiff_t>(count));
    for (auto const& stage_op : stage_ops.first(count)) {
      candidate.fallback.operators.push_back(stage_op->copy());
    }
    if (op.absorb_aggregation(candidate)) {
      return count;
    }
  }
  return None{};
}

} // namespace

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
  // Let a source take over the stages that follow it, such as filters,
  // projections, an aggregation, sorts, and limits. This runs after the walk,
  // so that the stages see the predicates as the walk left them, and before
  // aggregations are offered to the other sources below.
  auto& ops = replacement.operators;
  for (auto k = size_t{0}; k + 1 < ops.size(); ++k) {
    if (not ops[k]->takes_query_stages()) {
      continue;
    }
    auto [entries, end] = collect_chain(std::span{ops}.subspan(k + 1), octx);
    auto chain = QueryChain{.stages = {}, .end = std::move(end)};
    for (auto const& entry : entries) {
      chain.stages.push_back(entry.stage);
    }
    auto absorption = ops[k]->absorb(chain, octx);
    TENZIR_ASSERT(absorption.prefix <= entries.size());
    if (absorption.prefix > 0) {
      auto const last = k + 1 + entries[absorption.prefix - 1].last;
      ops.erase(ops.begin() + detail::narrow<std::ptrdiff_t>(k + 1),
                ops.begin() + detail::narrow<std::ptrdiff_t>(last + 1));
    }
  }
  // Let an operator compute an aggregation that follows it, either directly
  // or behind operators that can rewrite the aggregation over their input,
  // such as renames. This runs after the walk, when every operator has
  // absorbed what it can, so that predicates kept between the operators
  // prevent the fusion. The fallback replays the skipped operators.
  for (auto j = size_t{1}; j < ops.size(); ++j) {
    auto aggregation = ops[j]->aggregation();
    // The source may also take over the stages that follow the aggregation,
    // as many as it accepts, which then leave the pipeline, too.
    auto stages = aggregation
                    ? aggregation_stages(std::span{ops}.subspan(j + 1), octx)
                    : std::vector<QueryStage>{};
    for (auto k = j; aggregation and k > 0;) {
      --k;
      if (auto absorbed = absorb_with_stages(*ops[k], *aggregation, stages,
                                             std::span{ops}.subspan(j + 1))) {
        ops.erase(ops.begin() + detail::narrow<std::ptrdiff_t>(k + 1),
                  ops.begin()
                    + detail::narrow<std::ptrdiff_t>(j + 1 + *absorbed));
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

auto ir::Operator::query_stage(OptimizeCtx const& octx) const
  -> Option<QueryStage> {
  TENZIR_UNUSED(octx);
  return None{};
}

auto ir::Operator::takes_query_stages() const -> bool {
  return false;
}

auto ir::Operator::absorb(QueryChain const& chain, OptimizeCtx const& octx)
  -> Absorption {
  TENZIR_UNUSED(chain, octx);
  return {};
}

auto ir::substitute(QueryPlan& plan, substitute_ctx ctx) -> failure_or<void> {
  for (auto& stage : plan) {
    if (auto* filter = try_as<QueryStage::Filter>(stage.kind)) {
      TRY(filter->predicate.substitute(ctx));
    }
  }
  return {};
}

auto ir::OptimizeAggregation::substitute(substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  TRY(fallback.substitute(ctx, instantiate));
  TRY(ir::substitute(stages, ctx));
  return {};
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
