//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/exec.hpp"

#include "tenzir/async/executor.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/package.hpp"
#include "tenzir/pipeline.hpp"
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
#include <caf/config_value.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduler.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <folly/coro/BlockingWait.h>
#include <tsl/robin_set.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
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

/// Monotonic counters for profiling channel throughput.
struct ChannelStats {
  /// We group/align by in and out here, because that is the grouping in which
  /// these are written.
  struct alignas(std::hardware_destructive_interference_size) data {
    std::atomic<size_t> bytes{0};
    std::atomic<size_t> signals{0};
    std::atomic<size_t> elements{0};
  };
  data in;
  data out;
};

/// Monotonic counters for profiling per-operator CPU usage.
struct ExecutorStats {
  std::atomic<int64_t> wall_ns{0};
  std::atomic<int64_t> cpu_ns{0};
  std::atomic<size_t> task_count{0};
};

/// Executor wrapper that forwards tasks to an inner executor while measuring
/// wall-clock time, thread CPU time, and task count.
class ProfilingExecutor : public folly::Executor {
public:
  ProfilingExecutor(folly::Executor::KeepAlive<> inner,
                    std::shared_ptr<ExecutorStats> stats)
    : inner_{std::move(inner)}, stats_{std::move(stats)} {
  }

  void add(folly::Func f) override {
    inner_->add([stats = stats_, f = std::move(f)]() mutable {
      auto wall_start = std::chrono::steady_clock::now();
      struct timespec cpu_start = {};
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_start);
      f();
      auto wall_end = std::chrono::steady_clock::now();
      struct timespec cpu_end = {};
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end);
      stats->wall_ns.fetch_add(
        std::chrono::duration_cast<std::chrono::nanoseconds>(wall_end
                                                             - wall_start)
          .count(),
        std::memory_order::relaxed);
      stats->cpu_ns.fetch_add((cpu_end.tv_sec - cpu_start.tv_sec)
                                  * 1'000'000'000LL
                                + (cpu_end.tv_nsec - cpu_start.tv_nsec),
                              std::memory_order::relaxed);
      stats->task_count.fetch_add(1, std::memory_order::relaxed);
    });
  }

private:
  folly::Executor::KeepAlive<> inner_;
  std::shared_ptr<ExecutorStats> stats_;
};

/// Data channel between two operators.
template <class T>
struct OpChannel {
public:
  explicit OpChannel(ChannelId id, size_t max_bytes,
                     std::shared_ptr<ChannelStats> stats)
    : id_{std::move(id)},
      max_bytes_{max_bytes},
      stats_{std::move(stats)},
      mutex_{Locked{}} {
  }

  auto send(OperatorMsg<T> x) -> Task<void> {
    auto lock = co_await mutex_.lock();
    auto bytes = count_bytes(x);
    // To also send big messages eventually, we allow to bypass the limit if the
    // queue is otherwise entirely free.
    auto free = lock->current_bytes + bytes;
    while (not free and lock->current_bytes + bytes > max_bytes_) {
      lock.unlock();
      co_await notify_send_.wait();
      lock = co_await mutex_.lock();
    }
    lock->current_bytes += bytes;
    // Update profiling counters.
    if (stats_) {
      stats_->in.bytes.fetch_add(bytes, std::memory_order::relaxed);
      if (is<Signal>(x)) {
        stats_->in.signals.fetch_add(1, std::memory_order::relaxed);
      } else {
        stats_->in.elements.fetch_add(1, std::memory_order::relaxed);
      }
    }
    lock->queue.push_back(std::move(x));
    notify_receive_.notify_one();
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
    // Update profiling counters.
    if (stats_) {
      stats_->out.bytes.fetch_add(bytes, std::memory_order::relaxed);
      if (is<Signal>(result)) {
        stats_->out.signals.fetch_add(1, std::memory_order::relaxed);
      } else {
        stats_->out.elements.fetch_add(1, std::memory_order::relaxed);
      }
    }
    notify_send_.notify_one();
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
  std::shared_ptr<ChannelStats> stats_;
  // TODO: This can surely be written better?
  Mutex<Locked> mutex_;
  Notify notify_send_;
  Notify notify_receive_;
  std::atomic<bool> sender_closed_ = false;
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
  std::shared_ptr<ChannelStats> stats;
  size_t max_bytes;
};

/// Collected profile for a single operator's executor.
struct ExecutorProfile {
  OpId id;
  std::shared_ptr<ExecutorStats> stats;
};

class TestExecCtx : public ExecCtx {
public:
  explicit TestExecCtx(bool profiling = false) : profiling_{profiling} {
  }

  auto get_channel_profiles() -> std::vector<ChannelProfile> {
    auto lock = std::scoped_lock{mutex_};
    return channel_profiles_;
  }

  auto get_executor_profiles() -> std::vector<ExecutorProfile> {
    auto lock = std::scoped_lock{mutex_};
    return executor_profiles_;
  }

  auto make_executor(OpId id) -> folly::Executor::KeepAlive<> override {
    if (not profiling_) {
      return {};
    }
    auto stats = std::make_shared<ExecutorStats>();
    auto exec = std::make_unique<ProfilingExecutor>(
      folly::getGlobalCPUExecutor(), stats);
    auto keep_alive = folly::Executor::getKeepAliveToken(exec.get());
    auto lock = std::scoped_lock{mutex_};
    executor_profiles_.push_back(ExecutorProfile{id, std::move(stats)});
    executors_.push_back(std::move(exec));
    return keep_alive;
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

private:
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
    auto stats = std::shared_ptr<ChannelStats>{};
    if (profiling_) {
      stats = std::make_shared<ChannelStats>();
      auto lock = std::scoped_lock{mutex_};
      channel_profiles_.push_back(ChannelProfile{id, stats, max_bytes});
    }
    auto shared = std::make_shared<OpChannel<T>>(std::move(id), max_bytes,
                                                 std::move(stats));
    return {OpPush<T>{shared}, OpPull<T>{shared}};
  }

  bool profiling_;
  std::mutex mutex_;
  std::vector<ChannelProfile> channel_profiles_;
  std::vector<ExecutorProfile> executor_profiles_;
  // Owns the ProfilingExecutor instances to keep them alive.
  std::vector<std::unique_ptr<ProfilingExecutor>> executors_;
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
    size_t current_bytes;
    size_t bytes_in;
    size_t bytes_out;
    size_t elements_in;
    size_t elements_out;
    size_t signals_in;
    size_t signals_out;
    size_t max_bytes;
  };
  std::vector<Channel> channels;
  struct Executor {
    std::string name;
    int64_t wall_ns;
    int64_t cpu_ns;
    size_t task_count;
  };
  std::vector<Executor> executors;
};

void write_profile(std::string const& path,
                   std::vector<ProfileSample> const& samples,
                   std::chrono::steady_clock::time_point t0) {
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
  // Sort operator names: main operators first, then (subs) entries, then
  // sub-pipeline operators. Within each group, sort lexicographically.
  auto op_sort_key
    = [](std::string const& name) -> std::pair<size_t, std::string const&> {
    auto is_subs = name.ends_with(" (subs)");
    auto base = is_subs ? std::string_view{name}.substr(0, name.size() - 7)
                        : std::string_view{name};
    auto depth = std::count(base.begin(), base.end(), '-');
    return {static_cast<size_t>(depth) * 2 + (is_subs ? 1 : 0), name};
  };
  std::sort(op_names.begin(), op_names.end(),
            [&](std::string const& a, std::string const& b) {
              return op_sort_key(a) < op_sort_key(b);
            });
  op_index.clear();
  for (size_t i = 0; i < op_names.size(); ++i) {
    op_index[op_names[i]] = i;
  }
  // pid 1 = Totals, pid 2..N+1 = per operator.
  static constexpr auto pid_totals = 1;
  auto op_pid = [](size_t idx) -> int {
    return static_cast<int>(idx) + 2;
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
  emit_process(pid_totals, "Totals", 0);
  for (size_t i = 0; i < op_names.size(); ++i) {
    emit_process(op_pid(i), op_names[i], static_cast<int>(i) + 1);
  }
  auto emit_counter = [&](char const* name, int pid, int64_t us, double val) {
    emit(fmt::format(
      R"pp({{"ph": "C", "name": "{}", "pid": {}, "ts": {}, "args": {{" ": {}}}}})pp",
      name, pid, us, val));
  };
  // Per-operator aggregated channel metrics for a single sample.
  struct OpChannelAgg {
    double bytes_in = 0;
    double bytes_out = 0;
    double elements_in = 0;
    double elements_out = 0;
    double signals_in = 0;
    double signals_out = 0;
    double buffer_bytes = 0;
    double buffer_batches = 0;
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
      if (auto si = op_index.find(sender_target); si != op_index.end()) {
        auto& agg = aggs[si->second];
        agg.bytes_in += static_cast<double>(ch.bytes_in);
        agg.elements_in += static_cast<double>(ch.elements_in);
        agg.signals_in += static_cast<double>(ch.signals_in);
        agg.buffer_bytes += static_cast<double>(ch.current_bytes);
        agg.buffer_batches
          += static_cast<double>(ch.elements_in - ch.elements_out);
      }
      if (auto ri = op_index.find(receiver_target); ri != op_index.end()) {
        auto& agg = aggs[ri->second];
        agg.bytes_out += static_cast<double>(ch.bytes_out);
        agg.elements_out += static_cast<double>(ch.elements_out);
        agg.signals_out += static_cast<double>(ch.signals_out);
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
        cur_execs[it->second] = PrevExec{ex.wall_ns, ex.cpu_ns, ex.task_count};
      }
    }
    // Totals accumulators.
    auto total_buffer_bytes = 0.0;
    auto total_buffer_batches = 0.0;
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
        emit_counter("A: CPU Active", pid, us, cpu_pct);
        emit_counter("B: CPU Active (cumulative)", pid, us, cpu_s);
        emit_counter("C: CPU Wall", pid, us, wall_pct);
        emit_counter("D: CPU Wall (cumulative)", pid, us, wall_s);
        emit_counter("E: Tasks/s", pid, us, tasks_per_s);
        emit_counter("F: Tasks (cumulative)", pid, us,
                     static_cast<double>(cur.task_count));
      }
      // Memory / channel metrics.
      total_buffer_bytes += agg.buffer_bytes;
      total_buffer_batches += agg.buffer_batches;
      auto prev_bi = oi < prev_aggs.size() ? prev_aggs[oi].bytes_in : 0.0;
      auto prev_bo = oi < prev_aggs.size() ? prev_aggs[oi].bytes_out : 0.0;
      auto prev_ei = oi < prev_aggs.size() ? prev_aggs[oi].elements_in : 0.0;
      auto prev_eo = oi < prev_aggs.size() ? prev_aggs[oi].elements_out : 0.0;
      auto prev_si = oi < prev_aggs.size() ? prev_aggs[oi].signals_in : 0.0;
      auto prev_so = oi < prev_aggs.size() ? prev_aggs[oi].signals_out : 0.0;
      auto bi_rate = (agg.bytes_in - prev_bi) / interval_s;
      auto bo_rate = (agg.bytes_out - prev_bo) / interval_s;
      emit_counter("G: Buffer (bytes)", pid, us, agg.buffer_bytes);
      emit_counter("H: Buffer (batches)", pid, us, agg.buffer_batches);
      emit_counter("I: Bytes In/s", pid, us, bo_rate);
      emit_counter("J: Bytes In (cumulative)", pid, us, agg.bytes_out);
      emit_counter("K: Bytes Out/s", pid, us, bi_rate);
      emit_counter("L: Bytes Out (cumulative)", pid, us, agg.bytes_in);
      emit_counter("M: Batches In/s", pid, us,
                   (agg.elements_out - prev_eo) / interval_s);
      emit_counter("N: Batches In (cumulative)", pid, us, agg.elements_out);
      emit_counter("O: Batches Out/s", pid, us,
                   (agg.elements_in - prev_ei) / interval_s);
      emit_counter("P: Batches Out (cumulative)", pid, us, agg.elements_in);
      emit_counter("Q: Signals In/s", pid, us,
                   (agg.signals_out - prev_so) / interval_s);
      emit_counter("R: Signals In (cumulative)", pid, us, agg.signals_out);
      emit_counter("S: Signals Out/s", pid, us,
                   (agg.signals_in - prev_si) / interval_s);
      emit_counter("T: Signals Out (cumulative)", pid, us, agg.signals_in);
    }
    // Totals across all operators.
    if (total_tasks > 0) {
      emit_counter("A: CPU Active", pid_totals, us, total_cpu_pct);
      emit_counter("B: CPU Active (cumulative)", pid_totals, us, total_cpu_s);
      emit_counter("C: CPU Wall", pid_totals, us, total_wall_pct);
      emit_counter("D: CPU Wall (cumulative)", pid_totals, us, total_wall_s);
      emit_counter("E: Tasks/s", pid_totals, us, total_tasks_per_s);
      emit_counter("F: Tasks (cumulative)", pid_totals, us,
                   static_cast<double>(total_tasks));
    }
    emit_counter("G: Buffer (bytes)", pid_totals, us, total_buffer_bytes);
    emit_counter("H: Buffer (batches)", pid_totals, us, total_buffer_batches);
    prev_aggs = std::move(aggs);
    prev_execs = std::move(cur_execs);
  }
  f << "\n  ]\n}\n";
}

auto run_plan(std::vector<AnyOperator> ops, caf::actor_system& sys,
              DiagHandler& dh, std::optional<std::string> const& profile_path)
  -> Task<failure_or<void>> {
  LOGW("spawning plan with {} operators", ops.size());
  auto chain = OperatorChain<void, void>::try_from(std::move(ops));
  // TODO
  TENZIR_ASSERT(chain);
  auto exec_ctx = TestExecCtx{profile_path.has_value()};
  auto emit_fn = [](std::span<const metrics_snapshot_entry> entries) {
    for (auto const& e : entries) {
      TENZIR_INFO("metrics: key={} value={} direction={} bytes={}", e.label.key,
                  e.label.value,
                  e.direction == metrics_direction::read ? "read" : "write",
                  e.value);
    }
  };
  // Profiling: sample channel and executor stats periodically if requested.
  auto samples = std::vector<ProfileSample>{};
  auto stop_flag = std::atomic<bool>{false};
  auto sampler = std::optional<std::thread>{};
  auto t0 = std::chrono::steady_clock::now();
  if (profile_path) {
    sampler.emplace([&] {
      while (not stop_flag.load(std::memory_order::relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        auto channel_profiles = exec_ctx.get_channel_profiles();
        auto executor_profiles = exec_ctx.get_executor_profiles();
        if (channel_profiles.empty() and executor_profiles.empty()) {
          continue;
        }
        auto sample = ProfileSample{std::chrono::steady_clock::now(), {}, {}};
        for (auto& p : channel_profiles) {
          auto bi = p.stats->in.bytes.load(std::memory_order::relaxed);
          auto bo = p.stats->out.bytes.load(std::memory_order::relaxed);
          sample.channels.push_back({
            p.id.value,
            bi >= bo ? bi - bo : 0,
            bi,
            bo,
            p.stats->in.elements.load(std::memory_order::relaxed),
            p.stats->out.elements.load(std::memory_order::relaxed),
            p.stats->in.signals.load(std::memory_order::relaxed),
            p.stats->out.signals.load(std::memory_order::relaxed),
            p.max_bytes,
          });
        }
        for (auto& p : executor_profiles) {
          sample.executors.push_back({
            p.id.value,
            p.stats->wall_ns.load(std::memory_order::relaxed),
            p.stats->cpu_ns.load(std::memory_order::relaxed),
            p.stats->task_count.load(std::memory_order::relaxed),
          });
        }
        samples.push_back(std::move(sample));
      }
    });
  }
  LOGW("blocking on pipeline");
  co_await run_pipeline(std::move(*chain), exec_ctx, sys, dh,
                        std::move(emit_fn));
  LOGW("blocking on pipeline done");
  if (sampler) {
    stop_flag.store(true, std::memory_order::relaxed);
    sampler->join();
    write_profile(*profile_path, samples, t0);
  }
  co_return {};
}

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

auto run_plan_blocking(std::vector<AnyOperator> ops, caf::actor_system& sys,
                       diagnostic_handler& dh,
                       std::optional<std::string> const& profile_path)
  -> failure_or<void> {
  auto cancel_source = folly::CancellationSource{};
  auto diag_handler = ExecDiagHandler{dh, cancel_source};
  auto task
    = folly::coro::co_invoke([&] -> Task<AsyncResult<failure_or<void>>> {
        co_return co_await folly::coro::co_awaitTry(
          folly::coro::co_withCancellation(
            cancel_source.getToken(),
            run_plan(std::move(ops), sys, diag_handler, profile_path)));
      });
#if 0
  TENZIR_INFO("running pipeline on a single thread");
  auto result = folly::coro::blockingWait(std::move(task));
#else
  TENZIR_INFO("running pipeline on {} threads",
              folly::getGlobalCPUExecutorCounters().numThreads);
  auto result = folly::coro::blockingWait(folly::coro::co_withExecutor(
    folly::getGlobalCPUExecutor(), std::move(task)));
#endif
  LOGI("end blocking");
  if (result.is_cancelled()) {
    TRY(diag_handler.failure());
    panic("pipeline got cancelled without error");
  }
  return std::move(result).unwrap();
}

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
  // Start the actual execution.
  TRY(run_plan_blocking(std::move(spawned), sys, ctx, cfg.profile));
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
