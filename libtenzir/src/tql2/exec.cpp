//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/exec.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec/pipeline.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/finalize_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/package.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plan/operator.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/tokens.hpp"
#include "tenzir/try.hpp"

#include <arrow/util/utf8.h>
#include <caf/actor_from_state.hpp>
#include <caf/config_value.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduler.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <folly/coro/BlockingWait.h>
#include <tsl/robin_set.h>

#include <filesystem>
#include <map>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace tenzir {
namespace {

auto load_packages_for_exec(diagnostic_handler& dh, caf::actor_system& sys)
  -> failure_or<void> {
  auto package_dirs = std::vector<std::filesystem::path>{};
  for (const auto& dir : config_dirs(sys.config())) {
    package_dirs.emplace_back(dir / "packages");
  }
  const auto& cfg = caf::content(sys.config());
  if (const auto* value = caf::get_if(&cfg, "tenzir.package-dirs")) {
    if (const auto* list = caf::get_if<caf::config_value::list>(value)) {
      for (const auto& entry : *list) {
        if (const auto* str = caf::get_if<std::string>(&entry)) {
          package_dirs.emplace_back(*str);
          continue;
        }
        diagnostic::error("entries in 'tenzir.package-dirs' must be strings")
          .note("got value of type {}", entry.type_name())
          .emit(dh);
        return failure::promise();
      }
    } else {
      diagnostic::error("'tenzir.package-dirs' must be a list of strings")
        .emit(dh);
      return failure::promise();
    }
  }
  auto packages_by_id = std::map<std::string, package>{};
  auto had_errors = false;
  auto processed_dirs = std::unordered_set<std::filesystem::path>{};
  processed_dirs.reserve(package_dirs.size());
  for (const auto& base_dir : package_dirs) {
    if (not processed_dirs.insert(base_dir).second) {
      continue;
    }
    auto ec = std::error_code{};
    if (not std::filesystem::exists(base_dir, ec)) {
      if (ec) {
        diagnostic::error("{}", ec)
          .note("while trying to access {}", base_dir)
          .emit(dh);
        had_errors = true;
      }
      continue;
    }
    if (not std::filesystem::is_directory(base_dir, ec)) {
      if (ec) {
        diagnostic::error("{}", ec)
          .note("while checking status of {}", base_dir)
          .emit(dh);
        had_errors = true;
      }
      continue;
    }
    if (std::filesystem::exists(base_dir / "package.yaml", ec)) {
      if (ec) {
        diagnostic::error("{}", ec)
          .note("while checking status of {}", base_dir / "package.yaml")
          .emit(dh);
        had_errors = true;
      }
      auto pkg = package::load(base_dir, dh, true);
      if (not pkg) {
        had_errors = true;
        continue;
      }
      auto id = pkg->id;
      packages_by_id.insert_or_assign(id, std::move(*pkg));
      continue;
    }
    auto try_process_package = [&](const std::filesystem::path& pkg_dir) {
      auto pkg = package::load(pkg_dir, dh, true);
      if (not pkg) {
        had_errors = true;
        return true;
      }
      auto id = pkg->id;
      packages_by_id.insert_or_assign(id, std::move(*pkg));
      return true;
    };
    if (std::filesystem::exists(base_dir / "package.yaml")) {
      try_process_package(base_dir);
      continue;
    }
    auto dir_it = std::filesystem::directory_iterator{base_dir, ec};
    if (ec) {
      diagnostic::error("{}", ec)
        .note("while enumerating packages in {}", base_dir)
        .emit(dh);
      had_errors = true;
      continue;
    }
    for (const auto& entry : dir_it) {
      auto entry_ec = std::error_code{};
      if (not entry.is_directory(entry_ec)) {
        if (entry_ec) {
          diagnostic::error("{}", entry_ec)
            .note("while checking status of {}", entry.path())
            .emit(dh);
          had_errors = true;
        }
        continue;
      }
      if (std::filesystem::exists(entry.path() / "package.yaml")) {
        try_process_package(entry.path());
      }
    }
  }
  if (had_errors) {
    return failure::promise();
  }
  auto normalized_to_id = std::unordered_map<std::string, std::string>{};
  normalized_to_id.reserve(packages_by_id.size());
  auto modules = std::vector<std::unique_ptr<module_def>>{};
  modules.reserve(packages_by_id.size());
  for (auto& [id, pkg] : packages_by_id) {
    if (pkg.operators.empty()) {
      continue;
    }
    auto normalized = package_module_name(pkg.id);
    auto [norm_it, inserted] = normalized_to_id.try_emplace(normalized, pkg.id);
    if (not inserted && norm_it->second != pkg.id) {
      diagnostic::error("package '{}' conflicts with '{}' after normalization "
                        "to '{}'",
                        pkg.id, norm_it->second, normalized)
        .emit(dh);
      had_errors = true;
      continue;
    }
    auto module = build_package_operator_module(pkg, dh);
    if (not module) {
      had_errors = true;
      continue;
    }
    if (not(*module)->defs.empty()) {
      modules.emplace_back(std::move(*module));
    }
  }
  if (had_errors) {
    return failure::promise();
  }
  auto guard = begin_registry_update();
  auto base = guard.current();
  auto next = base->clone();
  for (auto& module : modules) {
    for (auto& [name, set] : module->defs) {
      if (! set.mod) {
        TENZIR_ASSERT(! set.fn && ! set.op);
        continue;
      }
      next->replace_module(std::string{"packages"}, name, std::move(set.mod));
    }
  }
  guard.publish(std::shared_ptr<const registry>{std::move(next)});
  return {};
}

// TODO: This is a naive implementation and does not do scoping properly.
class let_resolver : public ast::visitor<let_resolver> {
public:
  explicit let_resolver(session ctx) : ctx_{ctx} {
  }

  void visit(ast::pipeline& x) {
    // TODO: Extraction + patch is probably a common pattern.
    for (auto it = x.body.begin(); it != x.body.end();) {
      auto let = std::get_if<ast::let_stmt>(&*it);
      if (not let) {
        visit(*it);
        ++it;
        continue;
      }
      visit(let->expr);
      auto value = const_eval(let->expr, ctx_);
      auto name = std::string{let->name_without_dollar()};
      if (value) {
        map_[std::move(name)] = tenzir::match(
          std::move(*value),
          [](auto x) -> ast::constant::kind {
            return x;
          },
          [](const pattern&) -> ast::constant::kind {
            TENZIR_UNREACHABLE();
          });
      } else {
        failure_ = value.error();
        map_[std::move(name)] = std::nullopt;
      }
      it = x.body.erase(it);
    }
  }

  void emit_not_found(const ast::dollar_var& var) {
    diagnostic::error("variable `{}` was not declared", var.id.name)
      .primary(var)
      .emit(ctx_);
    failure_ = failure::promise();
  }

  void visit(ast::selector& x) {
    auto* dollar_var = std::get_if<ast::dollar_var>(&x);
    if (not dollar_var) {
      enter(x);
      return;
    }
    auto it = map_.find(std::string{dollar_var->name_without_dollar()});
    if (it == map_.end()) {
      emit_not_found(*dollar_var);
      return;
    }
    if (not it->second) {
      // Variable exists but there was an error during evaluation.
      return;
    }
    // let bound variables cannot be on the lhs for now, because a let can only
    // bind a name to a value, but field_paths are not values yet.
    diagnostic::error("cannot assign to `{}` constant value",
                      dollar_var->id.name)
      .primary(*dollar_var)
      .emit(ctx_);
    failure_ = failure::promise();
  }

  void visit(ast::expression& x) {
    const auto* dollar_var = std::get_if<ast::dollar_var>(&*x.kind);
    if (not dollar_var) {
      enter(x);
      return;
    }
    auto it = map_.find(std::string{dollar_var->name_without_dollar()});
    if (it == map_.end()) {
      emit_not_found(*dollar_var);
      return;
    }
    if (not it->second) {
      // Variable exists but there was an error during evaluation.
      return;
    }
    x = ast::constant{*it->second, x.get_location()};
  }

  void load_balance(ast::invocation& x) {
    // We currently have some special casing here for the `load_balance`
    // operator. The `let_resolver` must somehow interact with operators that
    // modify the constant environment. There are probably better ways to do
    // this, but putting everything here was easy to do. We should reconsider
    // this strategy when introducing a second operator that can modify the
    // constant environment.
    const auto* docs = "https://docs.tenzir.com/tql2/operators/load_balance";
    const auto* usage = "load_balance over:list { … }";
    auto emit = [&](diagnostic_builder d) {
      if (d.inner().severity == severity::error) {
        failure_ = failure::promise();
      }
      std::move(d).docs(docs).usage(usage).emit(ctx_);
    };
    // Remove all the arguments, as we will be replacing them anyway.
    auto args = std::move(x.args);
    x.args.clear();
    if (args.empty()) {
      emit(
        diagnostic::error("expected two positional arguments").primary(x.op));
      return;
    }
    auto var = std::get_if<ast::dollar_var>(&*args[0].kind);
    if (not var) {
      emit(diagnostic::error("expected a `$`-variable").primary(args[0]));
      return;
    }
    if (args.size() < 2) {
      emit(diagnostic::error("expected a pipeline afterwards").primary(*var));
      return;
    }
    auto it = map_.find(std::string{var->name_without_dollar()});
    if (it == map_.end()) {
      emit_not_found(*var);
      return;
    }
    if (not it->second) {
      // Variable exists, but there was an error during evaluation.
      return;
    }
    auto pipe = std::get_if<ast::pipeline_expr>(&*args[1].kind);
    if (not pipe) {
      emit(
        diagnostic::error("expected a pipeline expression").primary(args[1]));
      return;
    }
    // We now expand the pipeline once for each entry in the list, replacing the
    // original variable with the list items.
    auto original = std::move(*it->second);
    auto entries = std::get_if<list>(&original);
    if (not entries) {
      auto got = original.match([]<class T>(const T&) {
        return type_kind::of<data_to_type_t<T>>;
      });
      emit(diagnostic::error("expected a list, got `{}`", got).primary(*var));
      *it->second = std::move(original);
      return;
    }
    if (entries->empty()) {
      emit(diagnostic::error("expected list to not be empty").primary(*var));
      *it->second = std::move(original);
      return;
    }
    for (auto& entry : *entries) {
      auto f = detail::overload{
        [](const auto& x) -> ast::constant::kind {
          return x;
        },
        [](const pattern&) -> ast::constant::kind {
          TENZIR_UNREACHABLE();
        },
      };
      auto constant = tenzir::match(entry, f);
      map_.insert_or_assign(std::string{var->name_without_dollar()}, constant);
      auto pipe_copy = *pipe;
      visit(pipe_copy);
      x.args.emplace_back(std::move(pipe_copy));
    }
    if (args.size() > 2) {
      emit(
        diagnostic::error("expected exactly two arguments, got {}", args.size())
          .primary(args[2]));
    }
    // Restore the original value in case it's used elsewhere.
    map_.insert_or_assign(std::string{var->name_without_dollar()},
                          std::move(original));
  }

  void visit(ast::invocation& x) {
    if (x.op.ref.resolved() and x.op.ref.segments().size() == 1
        and x.op.ref.segments()[0] == "load_balance") {
      // We special case this as a temporary solution.
      load_balance(x);
      return;
    }
    enter(x);
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto get_failure() -> failure_or<void> {
    return failure_;
  }

private:
  failure_or<void> failure_;
  std::unordered_map<std::string, std::optional<ast::constant::kind>> map_;
  session ctx_;
};

auto resolve_let_bindings(ast::pipeline& pipe, session ctx)
  -> failure_or<void> {
  auto resolver = let_resolver{ctx};
  resolver.visit(pipe);
  return resolver.get_failure();
}

auto compile_resolved(ast::pipeline&& pipe, session ctx)
  -> failure_or<pipeline> {
  auto fail = std::optional<failure>{};
  auto ops = std::vector<operator_ptr>{};
  for (auto& stmt : pipe.body) {
    auto result = stmt.match(
      [&](ast::invocation& x) -> failure_or<void> {
        // TODO: Where do we check that this succeeds?
        TRY(auto op, ctx.reg().get(x).make(
                       operator_factory_plugin::invocation{
                         std::move(x.op),
                         std::move(x.args),
                       },
                       ctx));
        TENZIR_ASSERT(op);
        ops.push_back(std::move(op));
        return {};
      },
      [&](ast::assignment& x) -> failure_or<void> {
#if 0
        // TODO: Cannot do this right now (release typeid problem).
        auto assignments = std::vector<assignment>();
        assignments.push_back(std::move(x));
        ops.push_back(std::make_unique<set_operator>(std::move(assignments)));
#else
        auto plugin = plugins::find<operator_factory_plugin>("tql2.set");
        TENZIR_ASSERT(plugin);
        auto args = std::vector<ast::expression>{};
        args.emplace_back(std::move(x));
        TRY(auto op, plugin->make(
                       operator_factory_plugin::invocation{
                         ast::entity{{ast::identifier{std::string{"set"},
                                                      location::unknown}}},
                         std::move(args),
                       },
                       ctx));
        ops.push_back(std::move(op));
#endif
        return {};
      },
      [&](ast::if_stmt& x) -> failure_or<void> {
        // TODO: Same problem regarding instantiation outside of plugin.
        auto args = std::vector<ast::expression>{};
        args.reserve(3);
        args.push_back(std::move(x.condition));
        args.emplace_back(ast::pipeline_expr{
          location::unknown, std::move(x.then), location::unknown});
        if (x.else_) {
          args.emplace_back(ast::pipeline_expr{
            location::unknown, std::move(x.else_->pipe), location::unknown});
        }
        auto plugin = plugins::find<operator_factory_plugin>("tql2.if");
        TENZIR_ASSERT(plugin);
        TRY(auto op,
            plugin->make(
              operator_factory_plugin::invocation{
                ast::entity{{ast::identifier{std::string{"if"}, x.if_kw}}},
                std::move(args),
              },
              ctx));
        ops.push_back(std::move(op));
        return {};
      },
      [&](ast::match_stmt& x) -> failure_or<void> {
        diagnostic::error("`match` not yet implemented, try using `if` instead")
          .primary(x)
          .emit(ctx.dh());
        return failure::promise();
      },
      [&](ast::let_stmt&) -> failure_or<void> {
        TENZIR_UNREACHABLE();
      });
    if (result.is_error()) {
      fail = result.error();
    }
  }
  if (fail) {
    return *fail;
  }
  return tenzir::pipeline{std::move(ops)};
}

} // namespace

auto parse_and_compile(std::string_view source, session ctx)
  -> failure_or<pipeline> {
  TRY(auto ast, parse(source, ctx));
  return compile(std::move(ast), ctx);
}

auto compile(ast::pipeline&& pipe, session ctx) -> failure_or<pipeline> {
  TRY(resolve_entities(pipe, ctx));
  TRY(resolve_let_bindings(pipe, ctx));
  return compile_resolved(std::move(pipe), ctx);
}

auto dump_tokens(std::span<token const> tokens, std::string_view source)
  -> bool {
  auto last = size_t{0};
  auto has_error = false;
  for (auto& token : tokens) {
    fmt::print("{:>15} {:?}\n", token.kind,
               source.substr(last, token.end - last));
    last = token.end;
    has_error |= token.kind == token_kind::error;
  }
  return not has_error;
}

namespace {

auto run_pipeline(exec::pipeline_actor pipe, base_ctx ctx) -> failure_or<void> {
  auto self = caf::scoped_actor{ctx};
  self->monitor(pipe);
  auto started
    = self->mail(atom::start_v).request(pipe, caf::infinite).receive();
  if (not started) {
    if (started.error() != ec::silent) {
      // TODO: What do we do here?
      diagnostic::error("start failed: {}", started.error()).emit(ctx);
    }
    return failure::promise();
  }
  auto down_msg = std::optional<caf::down_msg>{};
  self->receive_while([&] {
    return not down_msg;
  })([&down_msg](caf::down_msg msg) {
    down_msg = std::move(msg);
  });
  TENZIR_ASSERT(down_msg);
  if (down_msg->reason) {
    diagnostic::error("failed: {}", down_msg->reason).emit(ctx);
    return failure::promise();
  }
  return {};
}

class RestoreId {
public:
  static auto make(uint32_t id) -> RestoreId {
    return RestoreId{id};
  }

  auto unwrap() const -> uint32_t {
    return id_;
  }

private:
  RestoreId(uint32_t id) : id_{id} {
  }

  uint32_t id_;
};

auto run_plan(plan::pipeline pipe, caf::actor_system& sys,
              diagnostic_handler& dh) -> Task<failure_or<void>> {
  TENZIR_WARN("spawning plan: {:#?}", use_default_formatter{pipe});
  auto ops = std::vector<AnyOperator>{};
  for (auto& op : std::move(pipe).unwrap()) {
    if (op->needs_node()) {
      // TODO: Send fragment of plan to other process, run multiple chains
      // connected by some transport layer (e.g., CAF, or maybe even just TCP).
      TENZIR_TODO();
    } else {
    }
    ops.push_back(std::move(*op).spawn());
  }
  auto chain = OperatorChain<void, void>::try_from(std::move(ops));
  // TODO
  TENZIR_ASSERT(chain);
  // auto [push_input, pull_input] = make_op_channel<void>(10);
  // auto [push_output, pull_output] = make_op_channel<void>(10);
  // co_await push_input(Signal::checkpoint);
  TENZIR_WARN("blocking on pipeline");
  co_await run_pipeline(std::move(*chain),
                        // std::move(pull_input), std::move(push_output),
                        sys, dh);
  co_return {};
}

auto run_plan_blocking(plan::pipeline pipe, caf::actor_system& sys,
                       diagnostic_handler& dh) -> failure_or<void> {
#if 1
  return folly::coro::blockingWait(run_plan(std::move(pipe), sys, dh));
#else
  TENZIR_WARN("running {}/{} threads",
              folly::getGlobalCPUExecutorCounters().numActiveThreads,
              folly::getGlobalCPUExecutorCounters().numThreads);
  return folly::coro::blockingWait(run_plan(std::move(pipe), sys, dh)
                                     .semi()
                                     .via(folly::getGlobalCPUExecutor()));
#endif
}

// TODO: failure_or<bool> is bad
auto exec_with_ir(ast::pipeline ast, const exec_config& cfg, session ctx,
                  caf::actor_system& sys) -> failure_or<bool> {
  // Transform the AST into IR.
  auto b_ctx = base_ctx{ctx.dh(), ctx.reg(), sys};
  // (void)b_ctx.system();
  auto c_ctx = compile_ctx::make_root(b_ctx);
  TRY(auto ir, std::move(ast).compile(c_ctx));
  if (cfg.dump_ir) {
    fmt::print("{:#?}\n", ir);
    return not ctx.has_failure();
  }
  // Instantiate the IR.
  auto sub_ctx = substitute_ctx{c_ctx, nullptr};
  TRY(ir.substitute(sub_ctx, true));
  if (cfg.dump_inst_ir) {
    fmt::print("{:#?}\n", ir);
    return not ctx.has_failure();
  }
  if (ir.operators.empty()) {
    // TODO
    diagnostic::error("empty pipeline is not supported yet").emit(ctx);
    return failure::promise();
  }
  // Type check the instantiated IR. Because we do not support implicit sources
  // anymore, the pipeline must start with `void` if it's well-formed. After
  // instantiation, the pipeline must know it's output type when given a fixed
  // input type.
  TRY(auto output, ir.infer_type(tag_v<void>, ctx));
  if (not output.has_value()) {
    // TODO: Improve?
    panic("expected pipeline to know it's output type after instantiation");
  }
  // Add implicit sink before optimization.
  if (output->is_not<void>()) {
    // TODO: Support bytes.
    auto sink_def = output->is<table_slice>() ? cfg.implicit_events_sink
                                              : cfg.implicit_bytes_sink;
    auto sink = parse_pipeline_with_bad_diagnostics(sink_def, ctx)
                  // TODO: Error handling.
                  .unwrap()
                  .compile(c_ctx)
                  .unwrap();
    ir.lets.insert(ir.lets.end(), std::move_iterator{sink.lets.begin()},
                   std::move_iterator{sink.lets.end()});
    ir.operators.insert(ir.operators.end(),
                        std::move_iterator{sink.operators.begin()},
                        std::move_iterator{sink.operators.end()});
    TRY(output, ir.infer_type(tag_v<void>, ctx));
    TENZIR_ASSERT(output.has_value());
    // TODO: This is a problem with the implicit sink config.
    if (not output->is<void>()) {
      diagnostic::error("last operator must close pipeline, but it returns {}",
                        operator_type_name(*output))
        // TODO: This location will be unknown.
        .primary(ir.operators.back()->main_location())
        .emit(ctx);
      return failure::promise();
    }
  }
  // Optimize the IR.
  auto opt
    = std::move(ir).optimize(ir::optimize_filter{}, event_order::ordered);
  // TODO: Can this happen?
  TENZIR_ASSERT(opt.filter.empty());
  ir = std::move(opt.replacement);
  if (cfg.dump_opt_ir) {
    fmt::print("{:#?}\n", ir);
    return not ctx.has_failure();
  }
  // Finalize the IR into something that we can execute.
  auto i_ctx = finalize_ctx{c_ctx};
  TRY(auto finalized, std::move(ir).finalize(tag_v<void>, i_ctx));
  if (cfg.dump_finalized) {
    fmt::print("{:#?}\n", use_default_formatter(finalized));
    return not ctx.has_failure();
  }
  // Do not proceed to execution if there has been an error.
  if (ctx.has_failure()) {
    return false;
  }
  // Start the actual execution.
#if 1
  TRY(run_plan_blocking(std::move(finalized), sys, ctx));
  return true;
#else
  auto exec = exec::make_pipeline(
    std::move(finalized), exec::pipeline_settings{}, std::nullopt, b_ctx);
  return run_pipeline(std::move(exec), b_ctx).is_success();
#endif
}

auto exec_restore2(std::string_view job_id, RestoreId restore_id,
                   caf::actor_system& sys, diagnostic_handler& dh) {
  // 1. Get `plan::pipeline` to spawn operators again.
  // TODO: How do the operators with subpipelines get restored?
}

// TODO: Source for diagnostic handler?
auto exec_restore(std::span<const std::byte> plan_bytes,
                  exec::checkpoint_reader_actor checkpoint_reader, base_ctx ctx)
  -> failure_or<void> {
  auto f = caf::binary_deserializer{
    caf::const_byte_span{plan_bytes.data(), plan_bytes.size()}};
  auto plan = plan::pipeline{};
  auto ok = f.apply(plan);
  TENZIR_ASSERT(ok);
  auto exec = exec::make_pipeline(std::move(plan), exec::pipeline_settings{},
                                  std::move(checkpoint_reader), ctx);
  return run_pipeline(std::move(exec), ctx);
}

} // namespace

auto exec2(std::string_view source, diagnostic_handler& dh,
           const exec_config& cfg, caf::actor_system& sys) -> bool {
  auto result = std::invoke([&]() -> failure_or<bool> {
    TRY(load_packages_for_exec(dh, sys));
    auto provider = session_provider::make(dh);
    auto ctx = provider.as_session();
    TRY(validate_utf8(source, ctx));
    auto tokens = tokenize_permissive(source);
    if (cfg.dump_tokens) {
      return dump_tokens(tokens, source);
    }
    TRY(verify_tokens(tokens, ctx));
    TRY(auto parsed, parse(tokens, source, ctx));
    if (cfg.dump_ast) {
      fmt::print("{:#?}\n", parsed);
      return not ctx.has_failure();
    }
    if (cfg.dump_ir or cfg.dump_inst_ir or cfg.dump_opt_ir or cfg.dump_finalized
        or true) {
      // This new code path will eventually supersede the current one.
      return exec_with_ir(std::move(parsed), cfg, ctx, sys);
    }
    TRY(auto pipe, compile(std::move(parsed), ctx));
    if (cfg.dump_pipeline) {
      fmt::print("{:#?}\n", pipe);
      return not ctx.has_failure();
    }
    if (ctx.has_failure()) {
      // Do not proceed to execution if there has been an error.
      return false;
    }
    auto pipes = std::vector<pipeline>{};
    if (not cfg.multi) {
      pipes.push_back(std::move(pipe));
    } else {
      auto split = std::move(pipe).split_at_void();
      if (not split) {
        diagnostic::error(split.error()).emit(ctx);
        return false;
      }
      pipes = std::move(*split);
    }
    for (auto& pipe : pipes) {
      auto result
        = exec_pipeline(std::move(pipe), std::string{source}, ctx, cfg, sys);
      if (not result) {
        if (result.error() != ec::silent) {
          diagnostic::error(result.error()).emit(ctx);
        }
        return false;
      }
      if (ctx.has_failure()) {
        return false;
      }
    }
    return true;
  });
  return result ? *result : false;
}

} // namespace tenzir
