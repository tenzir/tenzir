//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/exec.hpp"

#include "tenzir/arc.hpp"
#include "tenzir/async/executor.hpp"
#include "tenzir/async/fused.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/execution_node_name_guard.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/package.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/session.hpp"
#include "tenzir/si_literals.hpp"
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
#include <caf/anon_mail.hpp>
#include <caf/config_value.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduler.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <folly/Demangle.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <tsl/robin_set.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace tenzir {

using namespace tenzir::si_literals;

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
      diagnostic::error("package '{}' conflicts with '{}' after "
                        "normalization "
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
    // let bound variables cannot be on the lhs for now, because a let can
    // only bind a name to a value, but field_paths are not values yet.
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
    // We now expand the pipeline once for each entry in the list, replacing
    // the original variable with the list items.
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

template <class T>
auto count_bytes(const OperatorMsg<T>& item) -> size_t {
  auto internal = sizeof(OperatorMsg<T>);
  auto external = match(
    item,
    [](const table_slice& slice) -> size_t {
      return slice.total_buffer_size();
    },
    [](const chunk_ptr& chunk) -> size_t {
      return chunk ? chunk->size() : 0;
    },
    [](const Signal&) -> size_t {
      return 0;
    });
  return internal + external;
}

template <class T>
auto count_events(const OperatorMsg<T>& item) -> size_t {
  return match(
    item,
    [](const table_slice& slice) -> size_t {
      return slice.rows();
    },
    [](const chunk_ptr&) -> size_t {
      return 0;
    },
    [](const Signal&) -> size_t {
      return 0;
    });
}

/// Monotonic counters for profiling channel throughput.
struct ChannelStats {
  /// We group/align by in and out here, because that is the grouping in which
  /// these are written.
  struct alignas(std::hardware_destructive_interference_size) data {
    std::atomic<size_t> bytes{0};
    std::atomic<size_t> signals{0};
    std::atomic<size_t> batches{0};
    std::atomic<size_t> events{0};
  };
  data in;
  data out;

  /// Backpressure intervals recorded by the sender.
  struct BackpressureEvent {
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point end;
  };

  bool record_backpressure = false;

  /// Record a backpressure event (thread-safe). No-op unless
  /// `record_backpressure` is set.
  void add_backpressure(std::chrono::steady_clock::time_point start,
                        std::chrono::steady_clock::time_point end) {
    if (not record_backpressure) {
      return;
    }
    auto lock = std::scoped_lock{bp_mutex_};
    backpressure_events_.push_back({start, end});
  }

  /// Return and clear all backpressure events under lock (thread-safe).
  auto drain_backpressure_events() -> std::vector<BackpressureEvent> {
    auto lock = std::scoped_lock{bp_mutex_};
    return std::exchange(backpressure_events_, {});
  }

private:
  mutable std::mutex bp_mutex_;
  std::vector<BackpressureEvent> backpressure_events_;
};

/// Push wrapper for fused channels that tracks channel metrics.
template <class T>
class MeteredFusedPush final : public Push<OperatorMsg<T>> {
public:
  MeteredFusedPush(FusedSender<OperatorMsg<T>> sender, Arc<ChannelStats> stats)
    : sender_{std::move(sender)}, stats_{std::move(stats)} {
  }

  auto operator()(OperatorMsg<T> x) -> Task<void> override {
    stats_->in.bytes.fetch_add(count_bytes(x), std::memory_order::relaxed);
    if (is<Signal>(x)) {
      stats_->in.signals.fetch_add(1, std::memory_order::relaxed);
    } else {
      stats_->in.batches.fetch_add(1, std::memory_order::relaxed);
      stats_->in.events.fetch_add(count_events(x), std::memory_order::relaxed);
    }
    co_await sender_.send(std::move(x));
  }

private:
  FusedSender<OperatorMsg<T>> sender_;
  Arc<ChannelStats> stats_;
};

/// Pull wrapper for fused channels that tracks channel metrics.
template <class T>
class MeteredFusedPull final : public Pull<OperatorMsg<T>> {
public:
  MeteredFusedPull(FusedReceiver<OperatorMsg<T>> receiver,
                   Arc<ChannelStats> stats)
    : receiver_{std::move(receiver)}, stats_{std::move(stats)} {
  }

  auto operator()() -> Task<Option<OperatorMsg<T>>> override {
    auto result = co_await receiver_.recv();
    if (result.is_some()) {
      stats_->out.bytes.fetch_add(count_bytes(*result),
                                  std::memory_order::relaxed);
      if (is<Signal>(*result)) {
        stats_->out.signals.fetch_add(1, std::memory_order::relaxed);
      } else {
        stats_->out.batches.fetch_add(1, std::memory_order::relaxed);
        stats_->out.events.fetch_add(count_events(*result),
                                     std::memory_order::relaxed);
      }
    }
    co_return result;
  }

private:
  FusedReceiver<OperatorMsg<T>> receiver_;
  Arc<ChannelStats> stats_;
};

/// Monotonic counters for profiling per-operator CPU usage.
struct ExecutorStats {
  std::atomic<int64_t> wall_ns{0};
  std::atomic<int64_t> cpu_ns{0};
  std::atomic<size_t> task_count{0};
};

/// Base class for `ProfilingExecutor` that holds the inner executor handle.
/// The specialization for `folly::IOExecutor` additionally forwards
/// `getEventBase()`.
template <class Handle>
class ProfilingExecutorBase : public Handle {
protected:
  explicit ProfilingExecutorBase(folly::Executor::KeepAlive<Handle> inner)
    : inner_{std::move(inner)} {
  }
  folly::Executor::KeepAlive<Handle> inner_;
};

template <>
class ProfilingExecutorBase<folly::IOExecutor> : public folly::IOExecutor {
public:
  auto getEventBase() -> folly::EventBase* override {
    return inner_->getEventBase();
  }

protected:
  explicit ProfilingExecutorBase(
    folly::Executor::KeepAlive<folly::IOExecutor> inner)
    : inner_{std::move(inner)} {
  }
  folly::Executor::KeepAlive<folly::IOExecutor> inner_;
};

/// Executor wrapper that forwards tasks to an inner executor while measuring
/// wall-clock time, thread CPU time, and task count. Also sets the
/// exec_node_name_guard thread-local for each continuation so that
/// per-operator tracking (e.g. allocator tagging) works in the async path.
template <class Handle = folly::Executor>
class ProfilingExecutor final : public ProfilingExecutorBase<Handle> {
public:
  static auto
  make(folly::Executor::KeepAlive<Handle> inner,
       Option<Arc<ExecutorStats>> stats, exec_node_name_guard::name_type name)
    -> folly::Executor::KeepAlive<Handle> {
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    auto* ptr = new ProfilingExecutor{std::move(inner), std::move(stats),
                                      std::move(name)};
    return folly::Executor::makeKeepAlive(static_cast<Handle*>(ptr));
  }

  void add(folly::Func f) override {
    this->inner_->add(
      [stats = stats_, name = name_, f = std::move(f)]() mutable {
        auto guard
          = exec_node_name_guard{name, exec_node_name_guard::type::folly};
        if (stats.is_none()) {
          f();
          return;
        }
        auto wall_start = std::chrono::steady_clock::now();
        struct timespec cpu_start = {};
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_start);
        f();
        auto wall_end = std::chrono::steady_clock::now();
        struct timespec cpu_end = {};
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end);
        auto wall_delta = std::chrono::duration_cast<std::chrono::microseconds>(
                            wall_end - wall_start)
                            .count();
        auto cpu_delta = (cpu_end.tv_sec - cpu_start.tv_sec) * 1'000'000'000LL
                         + (cpu_end.tv_nsec - cpu_start.tv_nsec);
        if (wall_delta > 1'000'000) {
          TENZIR_VERBOSE(
            "add(): wall={}us cpu={}ns total_cpu={}ns "
            "tasks={}",
            wall_delta, cpu_delta,
            (*stats)->cpu_ns.load(std::memory_order::relaxed) + cpu_delta,
            (*stats)->task_count.load(std::memory_order::relaxed) + 1);
        }
        (*stats)->wall_ns.fetch_add(
          std::chrono::duration_cast<std::chrono::nanoseconds>(wall_end
                                                               - wall_start)
            .count(),
          std::memory_order::relaxed);
        (*stats)->cpu_ns.fetch_add(cpu_delta, std::memory_order::relaxed);
        (*stats)->task_count.fetch_add(1, std::memory_order::relaxed);
      });
  }

  auto keepAliveAcquire() noexcept -> bool override {
    auto previous = refs_.fetch_add(1, std::memory_order::relaxed);
    if (previous == 0) {
      TENZIR_ERROR("unexpected acquire of executor keep-alive");
    }
    return true;
  }

  auto keepAliveRelease() noexcept -> void override {
    auto previous = refs_.fetch_sub(1, std::memory_order::release);
    if (previous == 0) {
      TENZIR_ERROR("unexpected release of executor keep-alive");
    } else if (previous == 1) {
      std::atomic_thread_fence(std::memory_order::acquire);
      delete this;
    }
  }

private:
  ProfilingExecutor(folly::Executor::KeepAlive<Handle> inner,
                    Option<Arc<ExecutorStats>> stats,
                    exec_node_name_guard::name_type name)
    : ProfilingExecutorBase<Handle>{std::move(inner)},
      stats_{std::move(stats)},
      name_{name} {
  }
  Option<Arc<ExecutorStats>> stats_;
  Atomic<size_t> refs_{1};
  exec_node_name_guard::name_type name_;
};

/// Data channel between two operators.
template <class T>
struct OpChannel {
public:
  explicit OpChannel(ChannelId id, size_t max_bytes,
                     Option<Arc<ChannelStats>> stats)
    : id_{std::move(id)},
      max_bytes_{max_bytes},
      stats_{std::move(stats)},
      mutex_{Locked{}} {
  }

  auto send(OperatorMsg<T> x) -> Task<void> {
    auto bytes = count_bytes(x);
    // Update profiling counters.
    if (stats_.is_some()) {
      (*stats_)->in.bytes.fetch_add(bytes, std::memory_order::relaxed);
      if (is<Signal>(x)) {
        (*stats_)->in.signals.fetch_add(1, std::memory_order::relaxed);
      } else {
        (*stats_)->in.batches.fetch_add(1, std::memory_order::relaxed);
        (*stats_)->in.events.fetch_add(count_events(x),
                                       std::memory_order::relaxed);
      }
    }
    auto lock = co_await mutex_.lock();
    lock->current_bytes += bytes;
    lock->queue.push_back(std::move(x));
    notify_receive_.notify_one();
    // Block sender if buffer exceeds capacity. Large messages may temporarily
    // exceed the limit, but the sender waits for drain before continuing.
    if (lock->current_bytes > max_bytes_) {
      auto bp_start = std::chrono::steady_clock::now();
      while (lock->current_bytes > max_bytes_) {
        lock.unlock();
        co_await notify_send_.wait();
        lock = co_await mutex_.lock();
      }
      if (stats_.is_some()) {
        auto bp_end = std::chrono::steady_clock::now();
        (*stats_)->add_backpressure(bp_start, bp_end);
      }
      // Cascade: wake the next blocked sender so it can re-check its
      // condition. This is needed when multiple senders (e.g., parallel
      // operator pull tasks) block on the same channel. Without this,
      // notify_one() from the receiver wakes one sender at a time, and
      // that sender may find the condition still unmet (other senders'
      // items remain), consuming the token without making progress.
      // The cascade propagates until all eligible senders have exited.
      notify_send_.notify_one();
    }
  }

  auto receive() -> Task<Option<OperatorMsg<T>>> {
    auto lock = co_await mutex_.lock();
    while (lock->queue.empty()) {
      if (sender_closed_.load(std::memory_order::acquire)) {
        co_return None{};
      }
      lock.unlock();
      co_await notify_receive_.wait();
      lock = co_await mutex_.lock();
    }
    auto result = std::move(lock->queue.front());
    lock->queue.pop_front();
    auto bytes = count_bytes(result);
    lock->current_bytes -= bytes;
    lock.unlock();
    // Update profiling counters.
    if (stats_.is_some()) {
      (*stats_)->out.bytes.fetch_add(bytes, std::memory_order::relaxed);
      if (is<Signal>(result)) {
        (*stats_)->out.signals.fetch_add(1, std::memory_order::relaxed);
      } else {
        (*stats_)->out.batches.fetch_add(1, std::memory_order::relaxed);
        (*stats_)->out.events.fetch_add(count_events(result),
                                        std::memory_order::relaxed);
      }
    }
    notify_send_.notify_one();
    // Cascade to the next blocked receiver (MPMC safety).
    notify_receive_.notify_one();
    co_return result;
  }

  /// Close the sending side of the channel synchronously.
  ///
  /// Safe to call from destructors. Wakes up any waiting receiver so that it
  /// can observe the closure.
  void close_sender() {
    sender_closed_.store(true, std::memory_order::release);
    notify_receive_.notify_one();
  }

private:
  struct Locked {
    size_t current_bytes = 0;
    std::deque<OperatorMsg<T>> queue;
  };

  ChannelId id_;
  size_t max_bytes_;
  Option<Arc<ChannelStats>> stats_;
  // TODO: This can surely be written better?
  Mutex<Locked> mutex_;
  Notify notify_send_;
  Notify notify_receive_;
  Atomic<bool> sender_closed_ = false;
};

template <class T>
class OpPush final : public Push<OperatorMsg<T>> {
public:
  explicit OpPush(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  ~OpPush() override {
    if (shared_) {
      shared_->close_sender();
    }
  }
  OpPush(OpPush&&) = default;
  OpPush& operator=(OpPush&&) = default;
  OpPush(const OpPush&) = delete;
  OpPush& operator=(const OpPush&) = delete;

  auto operator()(OperatorMsg<T> x) -> Task<void> override {
    TENZIR_ASSERT(shared_);
    return shared_->send(std::move(x));
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

template <class T>
class OpPull final : public Pull<OperatorMsg<T>> {
public:
  explicit OpPull(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  ~OpPull() override = default;
  OpPull(OpPull&&) = default;
  OpPull& operator=(OpPull&&) = default;
  OpPull(const OpPull&) = delete;
  OpPull& operator=(const OpPull&) = delete;

  auto operator()() -> Task<Option<OperatorMsg<T>>> override {
    TENZIR_ASSERT(shared_);
    return shared_->receive();
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

/// Collected profile for a single channel.
struct ChannelProfile {
  ChannelId id;
  Arc<ChannelStats> stats;
  element_type_tag type;
};

/// Collected profile for a single operator's executor.
struct ExecutorProfile {
  OpId id;
  Arc<ExecutorStats> stats;
  std::string name;
};

/// Per-operator snapshot of cumulative counters from the previous tick,
/// used to compute deltas.
struct OpSnapshot {
  size_t bytes_in = 0;
  size_t bytes_out = 0;
  size_t batches_in = 0;
  size_t batches_out = 0;
  size_t events_in = 0;
  size_t events_out = 0;
  size_t signals_in = 0;
  size_t signals_out = 0;
  int64_t cpu_ns = 0;
  int64_t wall_ns = 0;
  size_t task_count = 0;
};

// Forward declaration for use in TestExecCtx::emit_metrics().
auto build_profiler_snapshot(std::span<ChannelProfile const> channel_profiles,
                             std::span<ExecutorProfile const> executor_profiles,
                             time timestamp,
                             std::unordered_map<OpId, OpSnapshot>& prev)
  -> ProfilerSnapshot;

class TestExecCtx final : public ExecCtx {
public:
  explicit TestExecCtx(Profiler const& profiler, bool is_hidden = false)
    : profiling_{not is<NoProfiler>(profiler)},
      record_backpressure_{is<PerfettoProfiler>(profiler)},
      metrics_receiver_{try_as<NodeProfiler>(profiler)
                          ? try_as<NodeProfiler>(profiler)->metrics
                          : metrics_receiver_actor{}},
      is_hidden_{is_hidden} {
  }

  /// Return the current set of channel profiles.
  ///
  /// Once a channel is deleted, its information will be returned from this
  /// function one final time before it gets erased. Thus, it is important to
  /// actually call this function if `profiling` is enabled.
  auto get_channel_profiles() -> std::vector<ChannelProfile> {
    auto lock = std::scoped_lock{mutex_};
    auto alive = channels_ | std::views::filter([](auto const& c) {
                   return c.stats.strong_count() > 1;
                 })
                 | std::ranges::to<std::vector>();
    return std::exchange(channels_, std::move(alive));
  }

  /// Return the current set of executor profiles.
  ///
  /// Once an executor is deleted, its information will be returned from this
  /// function one final time before it gets erased. Thus, it is important to
  /// actually call this function if `profiling` is enabled.
  auto get_executor_profiles() -> std::vector<ExecutorProfile> {
    auto lock = std::scoped_lock{mutex_};
    auto alive = executors_ | std::views::filter([](auto const& e) {
                   return e.stats.strong_count() > 1;
                 })
                 | std::ranges::to<std::vector>();
    return std::exchange(executors_, std::move(alive));
  }

  auto make_executor(OpId id, std::string name)
    -> folly::Executor::KeepAlive<> override {
    return wrap_executor(std::move(id), folly::getGlobalCPUExecutor(),
                         std::move(name));
  }

  auto make_io_executor(OpId id)
    -> folly::Executor::KeepAlive<folly::IOExecutor> override {
    return wrap_executor<folly::IOExecutor>(std::move(id),
                                            folly::getGlobalIOExecutor());
  }

  auto make_counter(MetricsLabel label, MetricsDirection direction,
                    MetricsVisibility visibility) -> MetricsCounter override {
    return metrics_->make_counter(label, direction, visibility);
  }

  auto metrics_receiver() const -> metrics_receiver_actor override {
    return metrics_receiver_;
  }

  auto is_hidden() const -> bool override {
    return is_hidden_;
  }

  auto take_metrics_snapshot() -> std::vector<MetricsSnapshotEntry> {
    return metrics_->take_snapshot();
  }

protected:
  auto make_void(ChannelId id) -> PushPull<OperatorMsg<void>> override {
    return make_profiled_channel<void>(std::move(id), void_limit);
  }

  auto make_events(ChannelId id)
    -> PushPull<OperatorMsg<table_slice>> override {
    return make_profiled_channel<table_slice>(std::move(id), events_limit);
  }

  auto make_bytes(ChannelId id) -> PushPull<OperatorMsg<chunk_ptr>> override {
    return make_profiled_channel<chunk_ptr>(std::move(id), bytes_limit);
  }

  auto make_fused_void(ChannelId id) -> PushPull<OperatorMsg<void>> override {
    return make_profiled_fused_channel<void>(std::move(id));
  }

  auto make_fused_events(ChannelId id)
    -> PushPull<OperatorMsg<table_slice>> override {
    return make_profiled_fused_channel<table_slice>(std::move(id));
  }

  auto make_fused_bytes(ChannelId id)
    -> PushPull<OperatorMsg<chunk_ptr>> override {
    return make_profiled_fused_channel<chunk_ptr>(std::move(id));
  }

private:
  /// Wraps an executor to contribute to the stats of the given operator.
  template <class Handle = folly::Executor>
  auto wrap_executor(OpId id, folly::Executor::KeepAlive<Handle> inner,
                     std::string name = {})
    -> folly::Executor::KeepAlive<Handle> {
    auto alloc_name = exec_node_name_guard::name_type{};
    std::copy_n(id.value.begin(), std::min(id.value.size(), alloc_name.size()),
                alloc_name.begin());
    auto stats = Option<Arc<ExecutorStats>>{};
    auto lock = std::scoped_lock{mutex_};
    if (profiling_) {
      // Look for an existing executor with the same ID to share stats with.
      for (auto& e : executors_) {
        if (e.id == id) {
          stats = e.stats;
          break;
        }
      }
      if (stats.is_none()) {
        stats = Arc<ExecutorStats>{std::in_place};
        executors_.push_back(ExecutorProfile{id, *stats, std::move(name)});
      }
    }
    return ProfilingExecutor<Handle>::make(std::move(inner), std::move(stats),
                                           alloc_name);
  }

  /// Create an operator channel with the specified memory limit and collect
  /// its profile.
  ///
  /// Note that the limit can be exceeded due to (a) pending writes to the
  /// queue which are still in memory but not yet pushed to the queue, and (b)
  /// very big individual items that exceed the total capacity by themselves,
  /// as we eventually let them through since we need to transmit them.
  template <class T>
  auto make_profiled_channel(ChannelId id, size_t max_bytes)
    -> PushPull<OperatorMsg<T>> {
    auto stats = Option<Arc<ChannelStats>>{};
    if (profiling_) {
      auto lock = std::scoped_lock{mutex_};
      // Look for an existing channel with the same ID to share stats with.
      for (auto& c : channels_) {
        if (c.id == id) {
          stats = c.stats;
          break;
        }
      }
      if (stats.is_none()) {
        stats = Arc<ChannelStats>{std::in_place};
        (*stats)->record_backpressure = record_backpressure_;
        channels_.push_back(ChannelProfile{id, *stats, tag_v<T>});
      }
    }
    auto shared = std::make_shared<OpChannel<T>>(std::move(id), max_bytes,
                                                 std::move(stats));
    return {OpPush<T>{shared}, OpPull<T>{shared}};
  }

  /// Create a fused channel and collect its profile.
  template <class T>
  auto make_profiled_fused_channel(ChannelId id) -> PushPull<OperatorMsg<T>> {
    auto stats = Option<Arc<ChannelStats>>{};
    if (profiling_) {
      auto lock = std::scoped_lock{mutex_};
      for (auto& c : channels_) {
        if (c.id == id) {
          stats = c.stats;
          break;
        }
      }
      if (stats.is_none()) {
        stats = Arc<ChannelStats>{std::in_place};
        channels_.push_back(ChannelProfile{id, *stats, tag_v<T>});
      }
    }
    auto [sender, receiver] = fused_channel<OperatorMsg<T>>();
    if (stats.is_some()) {
      return {MeteredFusedPush<T>{std::move(sender), *stats},
              MeteredFusedPull<T>{std::move(receiver), *stats}};
    }
    return FusedSenderReceiver<OperatorMsg<T>>{std::move(sender),
                                               std::move(receiver)}
      .into_push_pull();
  }

  Arc<PipelineMetrics> metrics_{std::in_place};
  bool profiling_;
  bool record_backpressure_;
  metrics_receiver_actor metrics_receiver_;
  bool is_hidden_;
  std::mutex mutex_;
  std::vector<ChannelProfile> channels_;
  std::vector<ExecutorProfile> executors_;
#if TENZIR_DEBUG_ASYNC
  // These numbers block the channel immediately for testing purposes.
  static constexpr auto void_limit = 0;
  static constexpr auto events_limit = 0;
  static constexpr auto bytes_limit = 0;
#else
  // Memory limit per channel type.
  static constexpr auto void_limit = 1_Ki;
  static constexpr auto events_limit = 100_Mi;
  static constexpr auto bytes_limit = 100_Mi;
#endif
};

struct ProfileSample {
  std::chrono::steady_clock::time_point time;
  struct Channel {
    std::string name;
    element_type_tag type;
    size_t bytes_in;
    size_t bytes_out;
    size_t batches_in;
    size_t batches_out;
    size_t events_in;
    size_t events_out;
    size_t signals_in;
    size_t signals_out;
  };
  std::vector<Channel> channels;
  struct Executor {
    std::string name;
    std::string op_name;
    int64_t wall_ns;
    int64_t cpu_ns;
    size_t task_count;
  };
  std::vector<Executor> executors;
};

void write_profile(
  std::string const& path, std::vector<ProfileSample> const& samples,
  std::chrono::steady_clock::time_point t0,
  std::unordered_map<std::string,
                     std::vector<ChannelStats::BackpressureEvent>> const&
    backpressure_events) {
  auto f = std::ofstream{path};
  if (not f) {
    TENZIR_WARN("failed to open profile output file: {}", path);
    return;
  }
  // Parse channel names ("A -> B") and collect unique operator names from
  // both channels and executors. Boundary markers ("_") are skipped.
  auto op_names = std::vector<std::string>{};
  auto op_index = std::unordered_map<std::string, size_t>{};
  auto add_op = [&](std::string const& name) {
    if (name == "_") {
      return;
    }
    if (op_index.emplace(name, op_names.size()).second) {
      op_names.push_back(name);
    }
  };
  struct ChannelEndpoints {
    std::string sender;
    std::string receiver;
  };
  // An operator B is a child of operator A if B's name starts with "A-",
  // meaning A spawned a sub-pipeline containing B.
  auto is_child_of
    = [](std::string const& child, std::string const& parent) -> bool {
    return child.size() > parent.size() and child.starts_with(parent)
           and child[parent.size()] == '-';
  };
  auto channel_endpoints = std::unordered_map<std::string, ChannelEndpoints>{};
  for (auto const& sample : samples) {
    for (auto const& ch : sample.channels) {
      if (channel_endpoints.contains(ch.name)) {
        continue;
      }
      auto sep = ch.name.find(" -> ");
      TENZIR_ASSERT(sep != std::string::npos);
      auto sender = ch.name.substr(0, sep);
      auto receiver = ch.name.substr(sep + 4);
      add_op(sender);
      add_op(receiver);
      channel_endpoints[ch.name] = {std::move(sender), std::move(receiver)};
    }
    for (auto const& ex : sample.executors) {
      add_op(ex.name);
    }
  }
  // Build operator type name lookup from executor samples.
  auto op_type_names = std::unordered_map<std::string, std::string>{};
  for (auto const& sample : samples) {
    for (auto const& ex : sample.executors) {
      if (not ex.op_name.empty()) {
        op_type_names.emplace(ex.name, ex.op_name);
      }
    }
  }
  // Create separate "(subs)" entries for operators that have cross-boundary
  // channels to/from their sub-pipelines.
  auto sub_op_name = [](std::string const& parent) -> std::string {
    return fmt::format("{} (subs)", parent);
  };
  for (auto const& [name, ep] : channel_endpoints) {
    if (ep.sender != "_" and ep.receiver != "_") {
      if (is_child_of(ep.receiver, ep.sender)) {
        add_op(sub_op_name(ep.sender));
      } else if (is_child_of(ep.sender, ep.receiver)) {
        add_op(sub_op_name(ep.receiver));
      }
    }
  }
  // Sort operator names with natural ordering: numeric segments are compared
  // as numbers so "0/2" comes before "0/10". Main operators come first, then
  // (subs) entries, then sub-pipeline operators.
  auto natural_less = [](std::string_view a, std::string_view b) -> bool {
    auto ai = size_t{0};
    auto bi = size_t{0};
    while (ai < a.size() and bi < b.size()) {
      auto a_digit = std::isdigit(static_cast<unsigned char>(a[ai]));
      auto b_digit = std::isdigit(static_cast<unsigned char>(b[bi]));
      if (a_digit and b_digit) {
        // Compare numeric segments by value.
        auto a_start = ai;
        auto b_start = bi;
        while (ai < a.size()
               and std::isdigit(static_cast<unsigned char>(a[ai]))) {
          ++ai;
        }
        while (bi < b.size()
               and std::isdigit(static_cast<unsigned char>(b[bi]))) {
          ++bi;
        }
        auto a_len = ai - a_start;
        auto b_len = bi - b_start;
        if (a_len != b_len) {
          return a_len < b_len;
        }
        auto cmp = a.substr(a_start, a_len).compare(b.substr(b_start, b_len));
        if (cmp != 0) {
          return cmp < 0;
        }
      } else {
        if (a[ai] != b[bi]) {
          return a[ai] < b[bi];
        }
        ++ai;
        ++bi;
      }
    }
    return a.size() < b.size();
  };
  auto op_sort_key = [](std::string const& name) -> std::pair<size_t, size_t> {
    auto is_subs = name.ends_with(" (subs)");
    auto base = is_subs ? std::string_view{name}.substr(0, name.size() - 7)
                        : std::string_view{name};
    auto depth = std::count(base.begin(), base.end(), '-');
    return {static_cast<size_t>(depth) * 2 + (is_subs ? 1 : 0), 0};
  };
  std::sort(op_names.begin(), op_names.end(),
            [&](std::string const& a, std::string const& b) {
              auto ka = op_sort_key(a);
              auto kb = op_sort_key(b);
              if (ka.first != kb.first) {
                return ka.first < kb.first;
              }
              return natural_less(a, b);
            });
  op_index.clear();
  for (size_t i = 0; i < op_names.size(); ++i) {
    op_index[op_names[i]] = i;
  }
  // Use high pids to avoid Perfetto heuristics for low pid values.
  static constexpr auto pid_totals = 1000;
  auto op_pid = [](size_t idx) -> int {
    return static_cast<int>(idx) + 1001;
  };
  // Write the JSON.
  f << "{\n  \"traceEvents\": [\n";
  auto first_event = true;
  auto emit = [&](std::string const& json) {
    if (not first_event) {
      f << ",\n";
    }
    first_event = false;
    f << "    " << json;
  };
  // Metadata events for process group names and sort order.
  // Append " |" so Perfetto's auto-appended pid is visually separated.
  auto emit_process = [&](int pid, std::string const& name, int sort_index) {
    emit(fmt::format(
      R"pp({{"ph": "M", "pid": {}, "name": "process_name", "args": {{"name": "{} |"}}}})pp",
      pid, name));
    emit(fmt::format(
      R"pp({{"ph": "M", "pid": {}, "name": "process_sort_index", "args": {{"sort_index": {}}}}})pp",
      pid, sort_index));
  };
  // Look up the operator type name for a given op name. The op name may have
  // a suffix like " (subs)", so we strip it to find the base ID.
  auto display_name = [&](std::string const& name) -> std::string {
    auto base = name;
    auto space = base.find(' ');
    if (space != std::string::npos) {
      base = base.substr(0, space);
    }
    auto it = op_type_names.find(base);
    if (it != op_type_names.end()) {
      return fmt::format("{}: {}", name, it->second);
    }
    return name;
  };
  emit_process(pid_totals, "Global", 0);
  for (size_t i = 0; i < op_names.size(); ++i) {
    emit_process(op_pid(i), display_name(op_names[i]), static_cast<int>(i) + 1);
  }
  auto emit_counter = [&](char const* name, int pid, int64_t us, double val) {
    emit(fmt::format(
      R"pp({{"ph": "C", "name": "{}", "pid": {}, "ts": {}, "args": {{" ": {}}}}})pp",
      name, pid, us, val));
  };
  // Per-operator aggregated channel metrics for a single sample.
  struct OpChannelAgg {
    size_t bytes_in = 0;
    size_t bytes_out = 0;
    size_t batches_in = 0;
    size_t batches_out = 0;
    size_t events_in = 0;
    size_t events_out = 0;
    size_t signals_in = 0;
    size_t signals_out = 0;
    size_t buffer_bytes = 0;
    size_t buffer_batches = 0;
    size_t buffer_events = 0;
    bool has_events_in = false;
    bool has_events_out = false;
    bool has_bytes_in = false;
    bool has_bytes_out = false;
  };
  // Build per-operator channel aggregates: "In" metrics go to the sender,
  // "Out" metrics go to the receiver. For cross-boundary channels (between a
  // parent operator and its sub-pipeline), the parent's side is routed to the
  // separate "(subs)" pid instead.
  auto aggregate_channels
    = [&](ProfileSample const& sample) -> std::vector<OpChannelAgg> {
    auto aggs = std::vector<OpChannelAgg>(op_names.size());
    for (auto const& ch : sample.channels) {
      auto it = channel_endpoints.find(ch.name);
      TENZIR_ASSERT(it != channel_endpoints.end());
      auto const& [sender, receiver] = it->second;
      // Determine where to route the sender's "In" metrics.
      auto sender_target = std::string{};
      if (sender != "_" and receiver != "_" and is_child_of(receiver, sender)) {
        sender_target = sub_op_name(sender);
      } else {
        sender_target = sender;
      }
      // Determine where to route the receiver's "Out" metrics.
      auto receiver_target = std::string{};
      if (sender != "_" and receiver != "_" and is_child_of(sender, receiver)) {
        receiver_target = sub_op_name(receiver);
      } else {
        receiver_target = receiver;
      }
      auto is_events = ch.type.is<table_slice>();
      auto is_bytes = ch.type.is<chunk_ptr>();
      if (auto si = op_index.find(sender_target); si != op_index.end()) {
        auto& agg = aggs[si->second];
        agg.bytes_in += ch.bytes_in;
        agg.batches_in += ch.batches_in;
        agg.events_in += ch.events_in;
        agg.signals_in += ch.signals_in;
        auto clamp_sub = [](size_t a, size_t b) {
          return a >= b ? a - b : 0;
        };
        agg.buffer_bytes += clamp_sub(ch.bytes_in, ch.bytes_out);
        agg.buffer_batches += clamp_sub(ch.batches_in, ch.batches_out);
        if (is_events) {
          agg.buffer_events += clamp_sub(ch.events_in, ch.events_out);
        }
        agg.has_events_out |= is_events;
        agg.has_bytes_out |= is_bytes;
      }
      if (auto ri = op_index.find(receiver_target); ri != op_index.end()) {
        auto& agg = aggs[ri->second];
        agg.bytes_out += ch.bytes_out;
        agg.batches_out += ch.batches_out;
        agg.events_out += ch.events_out;
        agg.signals_out += ch.signals_out;
        agg.has_events_in |= is_events;
        agg.has_bytes_in |= is_bytes;
      }
    }
    return aggs;
  };
  // Emit all tracks in a single pass over samples, keeping previous-sample
  // state for rate computation.
  static constexpr auto ns_to_s = 1.0 / 1'000'000'000.0;
  auto prev_aggs = std::vector<OpChannelAgg>{};
  struct PrevExec {
    int64_t wall_ns = 0;
    int64_t cpu_ns = 0;
    size_t task_count = 0;
  };
  auto prev_execs = std::vector<PrevExec>{};
  for (size_t i = 0; i < samples.size(); ++i) {
    auto const& sample = samples[i];
    auto us
      = std::chrono::duration_cast<std::chrono::microseconds>(sample.time - t0)
          .count();
    auto interval_s = double{};
    if (i == 0) {
      interval_s = static_cast<double>(us) / 1'000'000.0;
    } else {
      auto prev_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       samples[i - 1].time - t0)
                       .count();
      interval_s = static_cast<double>(us - prev_us) / 1'000'000.0;
    }
    if (interval_s <= 0.0) {
      interval_s = 0.001;
    }
    auto aggs = aggregate_channels(sample);
    // Build per-operator executor lookup for this sample.
    auto cur_execs = std::vector<PrevExec>(op_names.size());
    for (auto const& ex : sample.executors) {
      if (auto it = op_index.find(ex.name); it != op_index.end()) {
        auto& entry = cur_execs[it->second];
        entry.wall_ns += ex.wall_ns;
        entry.cpu_ns += ex.cpu_ns;
        entry.task_count += ex.task_count;
      }
    }
    // Totals accumulators.
    auto total_buffer_bytes = size_t{0};
    auto total_buffer_batches = size_t{0};
    auto total_buffer_events = size_t{0};
    auto total_wall_s = 0.0;
    auto total_cpu_s = 0.0;
    auto total_tasks = size_t{0};
    auto total_wall_pct = 0.0;
    auto total_cpu_pct = 0.0;
    auto total_tasks_per_s = 0.0;
    for (size_t oi = 0; oi < op_names.size(); ++oi) {
      auto pid = op_pid(oi);
      auto const& agg = aggs[oi];
      // CPU / executor metrics first.
      auto const& cur = cur_execs[oi];
      if (cur.wall_ns != 0 or cur.cpu_ns != 0 or cur.task_count != 0) {
        auto wall_s = static_cast<double>(cur.wall_ns) * ns_to_s;
        auto cpu_s = static_cast<double>(cur.cpu_ns) * ns_to_s;
        total_wall_s += wall_s;
        total_cpu_s += cpu_s;
        total_tasks += cur.task_count;
        auto const& prev = oi < prev_execs.size() ? prev_execs[oi] : PrevExec{};
        auto wall_pct = static_cast<double>(cur.wall_ns - prev.wall_ns)
                        * ns_to_s / interval_s;
        auto cpu_pct = static_cast<double>(cur.cpu_ns - prev.cpu_ns) * ns_to_s
                       / interval_s;
        auto tasks_per_s
          = static_cast<double>(cur.task_count - prev.task_count) / interval_s;
        total_wall_pct += wall_pct;
        total_cpu_pct += cpu_pct;
        total_tasks_per_s += tasks_per_s;
        emit_counter("A: CPU Active (%)", pid, us, cpu_pct * 100.0);
        emit_counter("B: CPU Active (s) (cumulative)", pid, us, cpu_s);
        emit_counter("C: CPU Wall (%)", pid, us, wall_pct * 100.0);
        emit_counter("D: CPU Wall (s) (cumulative)", pid, us, wall_s);
        emit_counter("E: Tasks/s", pid, us, tasks_per_s);
        emit_counter("F: Tasks (cumulative)", pid, us,
                     static_cast<double>(cur.task_count));
      }
      // Channel metrics — skip entirely for void-only operators.
      auto has_any_channel = agg.has_events_in or agg.has_events_out
                             or agg.has_bytes_in or agg.has_bytes_out;
      if (has_any_channel) {
        total_buffer_bytes += agg.buffer_bytes;
        total_buffer_batches += agg.buffer_batches;
        total_buffer_events += agg.buffer_events;
        auto d = [](size_t v) {
          return static_cast<double>(v);
        };
        auto rate = [&](size_t cur, size_t prev) {
          return d(cur - prev) / interval_s;
        };
        auto const& prev
          = oi < prev_aggs.size() ? prev_aggs[oi] : OpChannelAgg{};
        auto mb = [](size_t v) {
          return static_cast<double>(v) / 1'000'000.0;
        };
        auto mb_rate = [&](size_t cur, size_t prev) {
          return mb(cur - prev) / interval_s;
        };
        emit_counter("G: Buffer (MB)", pid, us, mb(agg.buffer_bytes));
        emit_counter("H: Buffer (batches)", pid, us, d(agg.buffer_batches));
        if (agg.has_events_out) {
          emit_counter("I: Buffer (events)", pid, us, d(agg.buffer_events));
        }
        emit_counter("J: MB In/s", pid, us,
                     mb_rate(agg.bytes_out, prev.bytes_out));
        emit_counter("K: MB In (cumulative)", pid, us, mb(agg.bytes_out));
        emit_counter("L: MB Out/s", pid, us,
                     mb_rate(agg.bytes_in, prev.bytes_in));
        emit_counter("M: MB Out (cumulative)", pid, us, mb(agg.bytes_in));
        emit_counter("N: Batches In/s", pid, us,
                     rate(agg.batches_out, prev.batches_out));
        emit_counter("O: Batches In (cumulative)", pid, us, d(agg.batches_out));
        emit_counter("P: Batches Out/s", pid, us,
                     rate(agg.batches_in, prev.batches_in));
        emit_counter("Q: Batches Out (cumulative)", pid, us, d(agg.batches_in));
        if (agg.has_events_in) {
          emit_counter("R: Events In/s", pid, us,
                       rate(agg.events_out, prev.events_out));
          emit_counter("S: Events In (cumulative)", pid, us, d(agg.events_out));
        }
        if (agg.has_events_out) {
          emit_counter("T: Events Out/s", pid, us,
                       rate(agg.events_in, prev.events_in));
          emit_counter("U: Events Out (cumulative)", pid, us, d(agg.events_in));
        }
        emit_counter("V: Signals In/s", pid, us,
                     rate(agg.signals_out, prev.signals_out));
        emit_counter("W: Signals Out/s", pid, us,
                     rate(agg.signals_in, prev.signals_in));
      }
    }
    // Totals across all operators.
    if (total_tasks > 0) {
      emit_counter("A: CPU Active (%)", pid_totals, us, total_cpu_pct * 100.0);
      emit_counter("B: CPU Active (s) (cumulative)", pid_totals, us,
                   total_cpu_s);
      emit_counter("C: CPU Wall (%)", pid_totals, us, total_wall_pct * 100.0);
      emit_counter("D: CPU Wall (s) (cumulative)", pid_totals, us,
                   total_wall_s);
      emit_counter("E: Tasks/s", pid_totals, us, total_tasks_per_s);
      emit_counter("F: Tasks (cumulative)", pid_totals, us,
                   static_cast<double>(total_tasks));
    }
    emit_counter("G: Buffer (MB)", pid_totals, us,
                 static_cast<double>(total_buffer_bytes) / 1'000'000.0);
    emit_counter("H: Buffer (batches)", pid_totals, us,
                 static_cast<double>(total_buffer_batches));
    emit_counter("I: Buffer (events)", pid_totals, us,
                 static_cast<double>(total_buffer_events));
    prev_aggs = std::move(aggs);
    prev_execs = std::move(cur_execs);
  }
  // Emit backpressure duration bars from channel profiles.
  // Backpressure is routed to the *receiver* operator (the bottleneck whose
  // input buffer filled up), using the same routing as "Out" metrics.
  auto bp_pids_emitted = std::unordered_set<int>{};
  auto has_any_bp = false;
  for (auto const& [channel_name, bp_events] : backpressure_events) {
    if (bp_events.empty()) {
      continue;
    }
    // Parse channel name to get sender/receiver.
    auto sep = channel_name.find(" -> ");
    if (sep == std::string::npos) {
      continue;
    }
    auto sender = channel_name.substr(0, sep);
    auto receiver = channel_name.substr(sep + 4);
    if (receiver == "_") {
      continue;
    }
    // Apply the same routing as "Out" metrics.
    auto receiver_target = std::string{};
    if (sender != "_" and is_child_of(sender, receiver)) {
      receiver_target = sub_op_name(receiver);
    } else {
      receiver_target = receiver;
    }
    auto it = op_index.find(receiver_target);
    if (it == op_index.end()) {
      continue;
    }
    auto pid = op_pid(it->second);
    auto op_name = display_name(receiver_target);
    // Emit thread name metadata once per pid.
    if (bp_pids_emitted.insert(pid).second) {
      emit(fmt::format(
        R"pp({{"ph": "M", "pid": {}, "tid": 1, "name": "thread_name", "args": {{"name": "Blocks Upstream"}}}})pp",
        pid));
    }
    if (not has_any_bp) {
      has_any_bp = true;
      emit(fmt::format(
        R"pp({{"ph": "M", "pid": {}, "tid": 1, "name": "thread_name", "args": {{"name": "Blocks Upstream"}}}})pp",
        pid_totals));
    }
    for (auto const& ev : bp_events) {
      auto start_us
        = std::chrono::duration_cast<std::chrono::microseconds>(ev.start - t0)
            .count();
      auto dur_us = std::chrono::duration_cast<std::chrono::microseconds>(
                      ev.end - ev.start)
                      .count();
      // Per-operator bar.
      emit(fmt::format(
        R"pp({{"ph": "X", "name": " ", "pid": {}, "tid": 1, "ts": {}, "dur": {}}})pp",
        pid, start_us, dur_us));
      // Global bar, named after the operator.
      emit(fmt::format(
        R"pp({{"ph": "X", "name": "{}", "pid": {}, "tid": 1, "ts": {}, "dur": {}}})pp",
        op_name, pid_totals, start_us, dur_us));
    }
  }
  f << "\n  ]\n}\n";
}

/// Aggregate channel and executor profiles into a `ProfilerSnapshot`.
///
/// Computes deltas against `prev` and updates it with the current values.
/// Entries in `prev` for operators no longer present are removed.
auto build_profiler_snapshot(std::span<ChannelProfile const> channel_profiles,
                             std::span<ExecutorProfile const> executor_profiles,
                             time timestamp,
                             std::unordered_map<OpId, OpSnapshot>& prev)
  -> ProfilerSnapshot {
  // Per-operator stats collected from channels and executors. Each group of
  // fields is written exactly once via `set()`.
  struct OpStats {
    std::string name;
    Option<size_t> input_bytes;
    Option<size_t> bytes_in;
    Option<size_t> bytes_out;
    Option<size_t> batches_in;
    Option<size_t> batches_out;
    Option<size_t> events_in;
    Option<size_t> events_out;
    Option<size_t> signals_in;
    Option<size_t> signals_out;
    Option<int64_t> cpu_ns;
    Option<int64_t> wall_ns;
    Option<size_t> task_count;
  };
  auto set = []<class T>(Option<T>& field, T value) {
    TENZIR_ASSERT(field.is_none());
    field = value;
  };
  auto ops = std::unordered_map<OpId, OpStats>{};
  auto is_child_of = [](OpId const& child, OpId const& parent) -> bool {
    return child.value.size() > parent.value.size()
           and child.value.starts_with(parent.value)
           and child.value[parent.value.size()] == '-';
  };
  // Collect channel stats per operator. Each operator gets its input from
  // the upstream channel and its output from the downstream channel.
  // Cross-boundary channels (between a parent and its sub-pipeline) are
  // attributed to the child operator, not the parent.
  for (auto const& prof : channel_profiles) {
    auto sep = prof.id.value.find(" -> ");
    TENZIR_ASSERT(sep != std::string::npos);
    auto sender = OpId{prof.id.value.substr(0, sep)};
    auto receiver = OpId{prof.id.value.substr(sep + 4)};
    // Skip the "_" side of boundary channels, and skip the parent side of
    // cross-boundary channels (parent <-> sub-pipeline child).
    auto skip_sender = sender.value == "_" or is_child_of(receiver, sender);
    auto skip_receiver = receiver.value == "_" or is_child_of(sender, receiver);
    auto bytes_in = prof.stats->in.bytes.load(std::memory_order::relaxed);
    auto bytes_out = prof.stats->out.bytes.load(std::memory_order::relaxed);
    auto batches_in = prof.stats->in.batches.load(std::memory_order::relaxed);
    auto batches_out = prof.stats->out.batches.load(std::memory_order::relaxed);
    auto events_in = prof.stats->in.events.load(std::memory_order::relaxed);
    auto events_out = prof.stats->out.events.load(std::memory_order::relaxed);
    auto signals_in = prof.stats->in.signals.load(std::memory_order::relaxed);
    auto signals_out = prof.stats->out.signals.load(std::memory_order::relaxed);
    auto clamp_sub = [](size_t a, size_t b) {
      return a >= b ? a - b : 0;
    };
    // Channel "in" = data pushed by sender = sender's output.
    if (not skip_sender) {
      auto& s = ops[sender];
      set(s.bytes_out, bytes_in);
      set(s.batches_out, batches_in);
      set(s.events_out, events_in);
      set(s.signals_out, signals_in);
    }
    // Channel "out" = data pulled by receiver = receiver's input.
    if (not skip_receiver) {
      auto& r = ops[receiver];
      set(r.bytes_in, bytes_out);
      set(r.batches_in, batches_out);
      set(r.events_in, events_out);
      set(r.signals_in, signals_out);
      set(r.input_bytes, clamp_sub(bytes_in, bytes_out));
    }
  }
  // Collect executor stats per operator.
  for (auto const& ex : executor_profiles) {
    auto& s = ops[ex.id];
    s.name = ex.name;
    set(s.cpu_ns, ex.stats->cpu_ns.load(std::memory_order::relaxed));
    set(s.wall_ns, ex.stats->wall_ns.load(std::memory_order::relaxed));
    set(s.task_count, ex.stats->task_count.load(std::memory_order::relaxed));
  }
  // Build operator entries with deltas against the previous snapshot.
  auto result = ProfilerSnapshot{};
  result.timestamp = timestamp;
  auto wall_interval_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            defaults::metrics_interval)
                            .count();
  auto get = []<class T>(Option<T> const& field) -> T {
    return field.unwrap_or_default();
  };
  auto delta = [](size_t cur, size_t prev) -> uint64_t {
    return static_cast<uint64_t>(cur >= prev ? cur - prev : cur);
  };
  for (auto const& [id, s] : ops) {
    auto cur = OpSnapshot{
      get(s.bytes_in),    get(s.bytes_out),   get(s.batches_in),
      get(s.batches_out), get(s.events_in),   get(s.events_out),
      get(s.signals_in),  get(s.signals_out), get(s.cpu_ns),
      get(s.wall_ns),     get(s.task_count),
    };
    auto& old = prev[id];
    // Compute CPU usage as percentage of wall-clock time.
    auto cpu_usage = 0.0;
    if (wall_interval_ns > 0) {
      auto delta_cpu_ns = get(s.cpu_ns) - old.cpu_ns;
      cpu_usage = static_cast<double>(delta_cpu_ns)
                  / static_cast<double>(wall_interval_ns) * 100.0;
    }
    result.operators.push_back(OperatorProfileEntry{
      .operator_id = id.value,
      .name = s.name,
      .input_bytes = static_cast<uint64_t>(get(s.input_bytes)),
      .cpu = cpu_usage,
      .task_count = delta(get(s.task_count), old.task_count),
      .bytes_in = delta(get(s.bytes_in), old.bytes_in),
      .bytes_out = delta(get(s.bytes_out), old.bytes_out),
      .batches_in = delta(get(s.batches_in), old.batches_in),
      .batches_out = delta(get(s.batches_out), old.batches_out),
      .events_in = delta(get(s.events_in), old.events_in),
      .events_out = delta(get(s.events_out), old.events_out),
      .signals_in = delta(get(s.signals_in), old.signals_in),
      .signals_out = delta(get(s.signals_out), old.signals_out),
    });
    old = cur;
  }
  // Remove entries for operators no longer present.
  std::erase_if(prev, [&](auto const& kv) {
    return not ops.contains(kv.first);
  });
  return result;
}

/// Emit operator_metric messages and profiler snapshots for a NodeProfiler.
void emit_node_metrics(NodeProfiler const& node, TestExecCtx& exec_ctx,
                       size_t num_ops, time start_time, time now,
                       std::unordered_map<OpId, OpSnapshot>& prev_snapshots) {
  auto entries = exec_ctx.take_metrics_snapshot();
  auto external_read_bytes = uint64_t{0};
  auto external_write_bytes = uint64_t{0};
  auto internal_read_bytes = uint64_t{0};
  auto internal_write_bytes = uint64_t{0};
  for (auto const& e : entries) {
    TENZIR_ASSERT(e.type != MetricsType::gauge);
    if (e.visibility == MetricsVisibility::external_) {
      if (e.direction == MetricsDirection::read) {
        external_read_bytes += e.value;
      } else {
        external_write_bytes += e.value;
      }
    } else {
      if (e.direction == MetricsDirection::read) {
        internal_read_bytes += e.value;
      } else {
        internal_write_bytes += e.value;
      }
    }
  }
  auto elapsed = now - start_time;
  // FIXME: This doesn't work if a pipeline mixes internal and external ingress
  // or egress. The diff that will be maintained in the pipeline manager will
  // mix up the two values.
  if (external_read_bytes > 0) {
    auto m = operator_metric{};
    m.operator_index = 0;
    m.operator_name = "source";
    m.internal = false;
    m.outbound_measurement.unit = "bytes";
    m.outbound_measurement.num_elements = external_read_bytes;
    m.time_total = elapsed;
    m.time_running = elapsed;
    caf::anon_mail(std::move(m)).send(node.metrics);
  }
  if (internal_read_bytes > 0) {
    auto m = operator_metric{};
    m.operator_index = 0;
    m.operator_name = "source";
    m.internal = true;
    m.outbound_measurement.unit = "bytes";
    m.outbound_measurement.num_elements = internal_read_bytes;
    m.time_total = elapsed;
    m.time_running = elapsed;
    caf::anon_mail(std::move(m)).send(node.metrics);
  }
  if (external_write_bytes > 0) {
    auto m = operator_metric{};
    // We use operator index 1 such that a pipeline with a single operator will
    // still use two different operator indices for ingress and egress. The code
    // in the pipeline manager cannot handle the case where it's the same.
    m.operator_index = 1;
    m.operator_name = "sink";
    m.internal = false;
    m.inbound_measurement.unit = "bytes";
    m.inbound_measurement.num_elements = external_write_bytes;
    m.time_total = elapsed;
    m.time_running = elapsed;
    caf::anon_mail(std::move(m)).send(node.metrics);
  }
  if (internal_write_bytes > 0) {
    auto m = operator_metric{};
    m.operator_index = 1;
    m.operator_name = "sink";
    m.internal = true;
    m.inbound_measurement.unit = "bytes";
    m.inbound_measurement.num_elements = internal_write_bytes;
    m.time_total = elapsed;
    m.time_running = elapsed;
    caf::anon_mail(std::move(m)).send(node.metrics);
  }
  // Always drain profiles to prune dead channels/executors, even when there
  // is no importer to send snapshots to. Without this, the profile vectors
  // would grow unboundedly for long-running pipelines with dynamic
  // subpipelines.
  auto channels = exec_ctx.get_channel_profiles();
  auto executors = exec_ctx.get_executor_profiles();
  if (node.importer.is_some()) {
    auto snapshot_time = floor(now, defaults::metrics_interval);
    auto snapshot = build_profiler_snapshot(channels, executors, snapshot_time,
                                            prev_snapshots);
    if (not snapshot.operators.empty()) {
      for (auto& slice :
           build_profiler_slices(snapshot, node.importer->pipeline_id)) {
        caf::anon_mail(std::move(slice)).send(node.importer->actor);
      }
    }
  }
}

/// Run the profiling side-task for the given profiler configuration.
/// Handles cooperative shutdown: performs a final emit/write on cancellation.
auto run_profiler(Profiler const& profiler, TestExecCtx& exec_ctx,
                  size_t num_ops) -> Task<void> {
  co_return co_await co_match(
    profiler,
    [](NoProfiler const&) -> Task<void> {
      co_return;
    },
    [&](NodeProfiler const& node) -> Task<void> {
      auto start_time = time::clock::now();
      auto prev_snapshots = std::unordered_map<OpId, OpSnapshot>{};
      try {
        while (true) {
          auto now = time::clock::now();
          emit_node_metrics(node, exec_ctx, num_ops, start_time, now,
                            prev_snapshots);
          auto boundary = floor(now, defaults::metrics_interval)
                          + defaults::metrics_interval;
          co_await folly::coro::sleep(
            std::chrono::duration_cast<folly::HighResDuration>(boundary - now));
        }
      } catch (folly::OperationCancelled const&) {
        emit_node_metrics(node, exec_ctx, num_ops, start_time,
                          time::clock::now(), prev_snapshots);
        throw;
      }
    },
    [&](PerfettoProfiler const& perfetto) -> Task<void> {
      auto samples = std::vector<ProfileSample>{};
      auto bp_events
        = std::unordered_map<std::string,
                             std::vector<ChannelStats::BackpressureEvent>>{};
      auto const t0 = std::chrono::steady_clock::now();
      // Snapshot current profiles into `samples` and drain backpressure
      // events. Called every tick and once more on cancellation.
      auto take_sample = [&] {
        auto channel_profiles = exec_ctx.get_channel_profiles();
        auto executor_profiles = exec_ctx.get_executor_profiles();
        for (auto& p : channel_profiles) {
          auto drained = p.stats->drain_backpressure_events();
          if (not drained.empty()) {
            auto& dest = bp_events[p.id.value];
            dest.insert(dest.end(), std::make_move_iterator(drained.begin()),
                        std::make_move_iterator(drained.end()));
          }
        }
        if (channel_profiles.empty() and executor_profiles.empty()) {
          return;
        }
        auto sample = ProfileSample{std::chrono::steady_clock::now(), {}, {}};
        sample.channels.reserve(channel_profiles.size());
        for (auto& p : channel_profiles) {
          sample.channels.push_back({
            p.id.value,
            p.type,
            p.stats->in.bytes.load(std::memory_order::relaxed),
            p.stats->out.bytes.load(std::memory_order::relaxed),
            p.stats->in.batches.load(std::memory_order::relaxed),
            p.stats->out.batches.load(std::memory_order::relaxed),
            p.stats->in.events.load(std::memory_order::relaxed),
            p.stats->out.events.load(std::memory_order::relaxed),
            p.stats->in.signals.load(std::memory_order::relaxed),
            p.stats->out.signals.load(std::memory_order::relaxed),
          });
        }
        sample.executors.reserve(executor_profiles.size());
        for (auto& p : executor_profiles) {
          sample.executors.push_back({
            p.id.value,
            p.name,
            p.stats->wall_ns.load(std::memory_order::relaxed),
            p.stats->cpu_ns.load(std::memory_order::relaxed),
            p.stats->task_count.load(std::memory_order::relaxed),
          });
        }
        samples.push_back(std::move(sample));
      };
      try {
        while (true) {
          co_await folly::coro::sleep(
            std::chrono::duration_cast<folly::HighResDuration>(
              std::chrono::milliseconds{100}));
          take_sample();
        }
      } catch (folly::OperationCancelled const&) {
        take_sample();
        write_profile(perfetto.path, samples, t0, bp_events);
        throw;
      }
    });
}

} // namespace

auto run_plan(OperatorChain<void, void> chain, caf::actor_system& sys,
              DiagHandler& dh, Profiler profiler, bool is_hidden)
  -> Task<failure_or<void>> {
  auto num_ops = chain.size();
  LOGW("spawning plan with {} operators", num_ops);
  auto exec_ctx = TestExecCtx{profiler, is_hidden};
  co_await async_scope([&](AsyncScope& scope) -> Task<void> {
    scope.spawn(run_profiler(profiler, exec_ctx, num_ops));
    LOGW("blocking on pipeline");
    co_await run_pipeline(std::move(chain), exec_ctx, sys, dh);
    LOGW("blocking on pipeline done");
    scope.cancel();
  });
  co_return {};
}

namespace {

/// Wraps a legacy `diagnostic_handler` into a `DiagHandler`.
class ExecDiagHandler final : public DiagHandler {
public:
  ExecDiagHandler(diagnostic_handler& dh, folly::CancellationSource& source)
    : dh_{dh}, cancel_source_{source} {
  }

  auto emit(diagnostic d) -> void override {
    // We make it thread-safe and deduplicating.
    auto lock = std::scoped_lock{mutex_};
    if (dedup_.insert(d)) {
      if (d.severity == severity::error) {
        failure_ = failure::promise();
        cancel_source_->requestCancellation();
      }
      dh_->emit(std::move(d));
    }
  }

  auto failure() -> failure_or<void> override {
    auto lock = std::scoped_lock{mutex_};
    return failure_;
  }

private:
  std::mutex mutex_;
  Ref<diagnostic_handler> dh_;
  Ref<folly::CancellationSource> cancel_source_;
  diagnostic_deduplicator dedup_;
  failure_or<void> failure_;
};

auto run_plan_blocking(OperatorChain<void, void> chain, caf::actor_system& sys,
                       diagnostic_handler& dh,
                       std::optional<std::string> const& profile_path)
  -> failure_or<void> {
  auto profiler = Profiler{};
  if (profile_path) {
    profiler = PerfettoProfiler{*profile_path};
  }
  auto cancel_source = folly::CancellationSource{};
  auto diag_handler = ExecDiagHandler{dh, cancel_source};
  auto task = folly::coro::co_invoke([&] -> Task<Option<failure_or<void>>> {
    co_return co_await catch_cancellation(folly::coro::co_withCancellation(
      cancel_source.getToken(), run_plan(std::move(chain), sys, diag_handler,
                                         std::move(profiler), false)));
  });
#if 1
  TENZIR_INFO("running pipeline on a single thread");
  auto result = folly::coro::blockingWait(std::move(task));
#else
  TENZIR_INFO("running pipeline on {} threads",
              folly::getGlobalCPUExecutorCounters().numThreads);
  auto result = folly::coro::blockingWait(folly::coro::co_withExecutor(
    folly::getGlobalCPUExecutor(), std::move(task)));
#endif
  LOGI("end blocking");
  TRY(diag_handler.failure());
  if (not result) {
    panic("pipeline got cancelled without error");
  }
  if (result->is_error()) {
    panic("got failure from run_plan but not in diagnostic handler");
  }
  return {};
}

} // namespace

auto build_profiler_slices(ProfilerSnapshot const& snapshot,
                           std::string_view pipeline_id)
  -> std::vector<table_slice> {
  static auto const profile_type = type{
    "tenzir.metrics.operator_profile",
    record_type{
      {"timestamp", time_type{}},
      {"pipeline_id", string_type{}},
      {"operator_id", string_type{}},
      {"name", string_type{}},
      {"input_bytes", uint64_type{}},
      {"cpu", double_type{}},
      {"task_count", uint64_type{}},
      {"bytes_in", uint64_type{}},
      {"bytes_out", uint64_type{}},
      {"batches_in", uint64_type{}},
      {"batches_out", uint64_type{}},
      {"events_in", uint64_type{}},
      {"events_out", uint64_type{}},
      {"signals_in", uint64_type{}},
      {"signals_out", uint64_type{}},
    },
    {{"internal"}},
  };
  auto result = std::vector<table_slice>{};
  if (not snapshot.operators.empty()) {
    auto builder = series_builder{profile_type};
    for (auto const& op : snapshot.operators) {
      auto row = builder.record();
      row.field("timestamp").data(snapshot.timestamp);
      row.field("pipeline_id").data(pipeline_id);
      row.field("operator_id").data(op.operator_id);
      row.field("name").data(op.name);
      row.field("input_bytes").data(op.input_bytes);
      row.field("cpu").data(op.cpu);
      row.field("task_count").data(op.task_count);
      row.field("bytes_in").data(op.bytes_in);
      row.field("bytes_out").data(op.bytes_out);
      row.field("batches_in").data(op.batches_in);
      row.field("batches_out").data(op.batches_out);
      row.field("events_in").data(op.events_in);
      row.field("events_out").data(op.events_out);
      row.field("signals_in").data(op.signals_in);
      row.field("signals_out").data(op.signals_out);
    }
    result.push_back(builder.finish_assert_one_slice());
  }
  return result;
}

namespace {

// TODO: failure_or<bool> is bad
auto exec_with_ir(ast::pipeline ast, const exec_config& cfg, session ctx,
                  caf::actor_system& sys) -> failure_or<bool> {
  // Transform the AST into IR.
  auto b_ctx = base_ctx{ctx.dh(), ctx.reg()};
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
    // Instantiate the sink (same as the main pipeline).
    TRY(sink.substitute(sub_ctx, true));
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
  // Spawn operators from the IR.
  auto spawned = std::move(ir).spawn(tag_v<void>);
  // Do not proceed to execution if there has been an error.
  if (ctx.has_failure()) {
    return false;
  }
  auto chain = OperatorChain<void, void>::try_from(std::move(spawned))
                 .expect("we already checked the type");
  // Start the actual execution.
  TRY(run_plan_blocking(std::move(chain), sys, ctx, cfg.profile));
  return true;
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
    if (cfg.neo or cfg.dump_ir or cfg.dump_inst_ir or cfg.dump_opt_ir) {
      // This new code path will eventually supersede the current one.
      return exec_with_ir(std::move(parsed), cfg, ctx, sys);
    }
    if (cfg.profile) {
      diagnostic::warning("`--profile` is only supported with `--neo`")
        .emit(ctx);
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
