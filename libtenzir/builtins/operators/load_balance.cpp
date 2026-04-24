//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/pipeline_executor.hpp"
#include "tenzir/scope_linked.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/view.hpp"

#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/anon_mail.hpp>

#include <algorithm>
#include <deque>

namespace tenzir::plugins::load_balance {

namespace {

using load_balancer_actor = caf::typed_actor<
  // Write events to a consumer pipeline, waiting for a read.
  auto(atom::write, table_slice events)->caf::result<void>,
  // Read events, waiting for a write.
  auto(atom::read)->caf::result<table_slice>>
  // Handle metrics of the nested pipelines.
  ::extend_with<metrics_receiver_actor>
  // Handle diagnostics of the nested pipelines.
  ::extend_with<receiver_actor<diagnostic>>;

/// Load balancing is currently done in a naive, yet hopefully effective way:
/// - Every subpipeline configuration gets its own executor.
/// - When the source of that pipeline is pulled from, it requests a batch from
///   the load balancer.
/// - The input of the `load_balance` operator is sent to the load balancer as
///   well in order to be forwarded to these read requests.
/// - Read requests are fulfilled in a FIFO manner, guaranteeing some degree of
///   fairness.
/// - The write operation only returns when the batch has been read from.
/// - Because of the implicit buffering between operators, upstream can still
///   continue even if the write is being blocked.
/// - We currently hand out batches exactly as they come in. Thus, their size
///   can vary significantly, producing an uneven load across the instances.
/// If our handover strategy here unexpectedly turns out to be a bottleneck,
/// then it should not be too hard to switch to a different mechanism.
struct load_balancer_state {
  [[maybe_unused]] static constexpr auto name = "load-balancer";

  load_balancer_actor::pointer self{};
  shared_diagnostic_handler diagnostics;
  metrics_receiver_actor metrics;
  std::vector<pipeline_executor_actor> executors;
  std::deque<caf::typed_response_promise<table_slice>> reads;
  std::deque<std::pair<table_slice, caf::typed_response_promise<void>>> writes;
  bool finished = false;
  uint64_t operator_index{};
  std::string pipeline_id;
  detail::stable_map<std::pair<uint64_t, uuid>, uuid> metrics_id_map;

  auto write(table_slice events) -> caf::result<void> {
    TENZIR_ASSERT(events.rows() > 0);
    if (not reads.empty()) {
      reads.front().deliver(std::move(events));
      reads.pop_front();
      return {};
    }
    writes.emplace_back(std::move(events), self->make_response_promise<void>());
    return writes.back().second;
  }

  auto read() -> caf::result<table_slice> {
    if (not writes.empty()) {
      auto events = std::move(writes.front().first);
      writes.front().second.deliver();
      writes.pop_front();
      return events;
    }
    if (finished) {
      return table_slice{};
    }
    reads.emplace_back(self->make_response_promise<table_slice>());
    return reads.back();
  }

  void finish() {
    if (finished) {
      return;
    }
    finished = true;
    TENZIR_DEBUG("load_balancer finished and marks {} outstanding reads done",
                 reads.size());
    // If there are any outstanding reads, we know that there are no remaining
    // writes. Thus it's fine to mark all outstanding reads as done.
    for (auto& read : reads) {
      read.deliver(table_slice{});
    }
    reads.clear();
  }
};

class load_balance_source final : public crtp_operator<load_balance_source> {
public:
  load_balance_source() = default;

  explicit load_balance_source(load_balancer_actor load_balancer)
    : load_balancer_{std::move(load_balancer)} {
  }

  auto name() const -> std::string override {
    return "internal-load-balance-source";
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    TENZIR_DEBUG("beginning execution of load_balance_source");
    TENZIR_ASSERT(load_balancer_);
    while (true) {
      auto result = table_slice{};
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::read_v)
        .request(load_balancer_, caf::infinite)
        .then(
          [&](table_slice slice) {
            result = std::move(slice);
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            // This should never happen.
            diagnostic::error("load balancer read failed: {}", err)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      // We signal completion with an empty table slice.
      if (result.rows() == 0) {
        TENZIR_DEBUG("load_balance_source detected end");
        break;
      }
      co_yield std::move(result);
    }
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_balance_source& x) -> bool {
    return f.apply(x.load_balancer_);
  }

private:
  load_balancer_actor load_balancer_;
};

/// Inserts the result of a function call if the key does not exist yet.
template <class Map, class Key, class F>
auto get_or_compute(Map& map, Key&& key, F&& f) -> decltype(auto) {
  class implicit_caller {
  public:
    explicit implicit_caller(F&& f) : f_{std::forward<F>(f)} {
    }

    explicit(false) operator typename Map::mapped_type() {
      return std::forward<F>(f_)();
    }

  private:
    F&& f_;
  };
  return map
    .try_emplace(std::forward<Key>(key), implicit_caller(std::forward<F>(f)))
    .first->second;
}

auto as_constant_kind(data const& value) -> ast::constant::kind {
  return match(
    value,
    []<class T>(T const& x) -> ast::constant::kind {
      if constexpr (std::same_as<T, pattern>) {
        TENZIR_UNREACHABLE();
      } else {
        return x;
      }
    });
}

struct LoadBalanceArgs {
  std::vector<ir::pipeline> pipes;
};

class LoadBalance final : public Operator<table_slice, void> {
public:
  explicit LoadBalance(LoadBalanceArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (started_) {
      co_return;
    }
    started_ = true;
    workers_.assign(args_.pipes.size(), Worker{});
    active_workers_ = workers_.size();
    for (auto i = size_t{0}; i < args_.pipes.size(); ++i) {
      co_await ctx.spawn_sub<table_slice>(
        data{detail::narrow<int64_t>(i)}, std::move(args_.pipes[i]));
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    auto const rows = input.rows();
    while (active_workers_ > 0) {
      auto worker = select_worker();
      TENZIR_ASSERT(worker);
      auto sub = ctx.get_sub(detail::narrow<int64_t>(*worker));
      if (not sub) {
        mark_inactive(*worker);
        continue;
      }
      auto& pipe = as<SubHandle<table_slice>>(*sub);
      auto result = co_await pipe.push(std::move(input));
      if (result.is_ok()) {
        workers_[*worker].rows_assigned += rows;
        co_return;
      }
      input = std::move(result).unwrap_err();
      mark_inactive(*worker);
    }
    done_ = true;
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto key_data = materialize(key);
    auto* index = try_as<int64_t>(key_data);
    TENZIR_ASSERT(index);
    mark_inactive(detail::narrow<size_t>(*index));
    co_return;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (auto sub = ctx.get_sub(detail::narrow<int64_t>(i))) {
        auto& pipe = as<SubHandle<table_slice>>(*sub);
        co_await pipe.close();
      }
      mark_inactive(i);
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  struct Worker {
    uint64_t rows_assigned = 0;
    bool active = true;
  };

  auto select_worker() const -> Option<size_t> {
    auto result = Option<size_t>{None{}};
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (not result
          or workers_[i].rows_assigned < workers_[*result].rows_assigned) {
        result = i;
      }
    }
    return result;
  }

  auto mark_inactive(size_t index) -> void {
    TENZIR_ASSERT(index < workers_.size());
    if (not workers_[index].active) {
      return;
    }
    workers_[index].active = false;
    TENZIR_ASSERT(active_workers_ > 0);
    --active_workers_;
    if (active_workers_ == 0) {
      done_ = true;
    }
  }

  LoadBalanceArgs args_;
  std::vector<Worker> workers_;
  size_t active_workers_ = 0;
  bool started_ = false;
  bool done_ = false;
};

class LoadBalanceIr final : public ir::Operator {
public:
  LoadBalanceIr() = default;

  LoadBalanceIr(location self, ast::dollar_var var, location pipe_location,
                ir::pipeline pipe)
    : self_{self},
      var_{std::move(var)},
      pipe_location_{pipe_location},
      pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "load_balance-ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    if (not instantiate) {
      if (not ctx.get(var_.let)) {
        return {};
      }
    }
    if (not pipes_.empty()) {
      if (instantiate and not pipes_instantiated_) {
        for (auto& pipe : pipes_) {
          TRY(pipe.substitute(ctx, true));
        }
        pipes_instantiated_ = true;
      }
      return {};
    }
    auto original = ctx.get(var_.let);
    if (not original) {
      diagnostic::error("expected a constant list")
        .primary(var_)
        .emit(ctx);
      return failure::promise();
    }
    auto* entries = try_as<list>(*original);
    if (not entries) {
      auto got = original->match([]<class T>(T const&) {
        return type_kind::of<data_to_type_t<T>>;
      });
      diagnostic::error("expected a list, got `{}`", got)
        .primary(var_)
        .emit(ctx);
      return failure::promise();
    }
    if (entries->empty()) {
      diagnostic::error("expected list to not be empty")
        .primary(var_)
        .emit(ctx);
      return failure::promise();
    }
    auto pipes = std::vector<ir::pipeline>{};
    pipes.reserve(entries->size());
    for (auto const& entry : *entries) {
      auto env = ctx.env();
      env.insert_or_assign(var_.let, as_constant_kind(entry));
      auto pipe = pipe_;
      TRY(pipe.substitute(ctx.with_env(&env), instantiate));
      pipes.push_back(std::move(pipe));
    }
    pipes_ = std::move(pipes);
    pipes_instantiated_ = instantiate;
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("operator expects events").primary(self_).emit(dh);
      return failure::promise();
    }
    for (auto const& pipe : pipes_) {
      TRY(auto output, pipe.infer_type(input, dh));
      if (not output) {
        return std::nullopt;
      }
      if (output->is_not<void>()) {
        diagnostic::error("pipeline must currently end with a sink")
          .primary(pipe_location_)
          .emit(dh);
        return failure::promise();
      }
    }
    return tag_v<void>;
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    TENZIR_UNUSED(filter, order);
    auto replacement = ir::pipeline{};
    replacement.operators.push_back(LoadBalanceIr{std::move(*this)});
    return {ir::optimize_filter{}, event_order::unordered,
            std::move(replacement)};
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return LoadBalance{LoadBalanceArgs{std::move(pipes_)}};
  }

  auto main_location() const -> location override {
    return self_;
  }

  friend auto inspect(auto& f, LoadBalanceIr& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("var", x.var_),
                              f.field("pipe_location", x.pipe_location_),
                              f.field("pipe", x.pipe_),
                              f.field("pipes", x.pipes_),
                              f.field("pipes_instantiated",
                                      x.pipes_instantiated_));
  }

private:
  location self_;
  ast::dollar_var var_;
  location pipe_location_;
  ir::pipeline pipe_;
  std::vector<ir::pipeline> pipes_;
  bool pipes_instantiated_ = false;
};

auto make_load_balancer(
  load_balancer_actor::stateful_pointer<load_balancer_state> self,
  std::vector<pipeline> pipes, std::string definition,
  shared_diagnostic_handler diagnostics, metrics_receiver_actor metrics,
  uint64_t operator_index, bool is_hidden, const node_actor& node,
  std::string pipeline_id) -> load_balancer_actor::behavior_type {
  TENZIR_DEBUG("spawning load balancer");
  self->attach_functor([] {
    TENZIR_DEBUG("destroyed load balancer");
  });
  self->state().self = self;
  self->state().diagnostics = std::move(diagnostics);
  self->state().metrics = std::move(metrics);
  self->state().operator_index = operator_index;
  self->state().pipeline_id = std::move(pipeline_id);
  self->state().executors.reserve(pipes.size());
  for (auto& pipe : pipes) {
    pipe.prepend(std::make_unique<load_balance_source>(self));
    auto has_terminal = false;
    TENZIR_DEBUG("spawning inner executor");
    auto executor
      = self->spawn(pipeline_executor, pipe, definition, self, self, node,
                    has_terminal, is_hidden, self->state().pipeline_id);
    self->monitor(
      executor, [self, source = executor->address()](const caf::error& err) {
        if (err.valid()) {
          diagnostic::error(err).emit(self->state().diagnostics);
        }
        auto it = std::ranges::find(self->state().executors, source,
                                    &pipeline_executor_actor::address);
        TENZIR_ASSERT(it != self->state().executors.end());
        self->state().executors.erase(it);
        if (self->state().executors.empty()) {
          // We are done, even if `not self->state().finished`.
          self->quit();
        }
      });
    executor->attach_functor([] {
      TENZIR_DEBUG("inner executor terminated");
    });
    self->mail(atom::start_v)
      .request(executor, caf::infinite)
      .then(
        []() {
          TENZIR_DEBUG("started inner pipeline successfully");
        },
        [self](const caf::error& err) {
          // This error should be enough to cause the outer pipeline to get
          // cleaned up.
          diagnostic::error(err).emit(self->state().diagnostics);
        });
    self->state().executors.push_back(std::move(executor));
  }
  return {
    [self](atom::write, table_slice& events) -> caf::result<void> {
      return self->state().write(std::move(events));
    },
    [self](atom::read) -> caf::result<table_slice> {
      return self->state().read();
    },
    [self](uint64_t op_index, uuid metrics_id,
           type& schema) -> caf::result<void> {
      auto id = get_or_compute(self->state().metrics_id_map,
                               std::pair{op_index, metrics_id}, [&] {
                                 return uuid::random();
                               });
      return self->mail(self->state().operator_index, id, schema)
        .delegate(self->state().metrics);
    },
    [self](uint64_t op_index, uuid metrics_id,
           record& metric) -> caf::result<void> {
      auto id
        = self->state().metrics_id_map.find(std::pair{op_index, metrics_id});
      TENZIR_ASSERT(id != self->state().metrics_id_map.end());
      return self
        ->mail(self->state().operator_index, id->second, std::move(metric))
        .delegate(self->state().metrics);
    },
    [](const operator_metric& op_metric) -> caf::result<void> {
      // There currently is no way to have subpipeline metrics.
      TENZIR_UNUSED(op_metric);
      return {};
    },
    [self](diagnostic& diagnostic) -> caf::result<void> {
      TENZIR_ASSERT(diagnostic.severity != severity::error);
      self->state().diagnostics.emit(std::move(diagnostic));
      return {};
    },
    [self](const caf::exit_msg& msg) {
      if (msg.reason != caf::exit_reason::user_shutdown) {
        // This should never happen.
        TENZIR_DEBUG("load balancer got unexpected exit msg: {}", msg.reason);
        self->quit(msg.reason);
        return;
      }
      // Let the sources know we are done and wait for their termination.
      self->state().finish();
    },
  };
}

class load_balance final : public crtp_operator<load_balance> {
public:
  load_balance() = default;

  explicit load_balance(std::vector<pipeline> pipes)
    : pipes_{std::move(pipes)} {
  }

  auto name() const -> std::string override {
    return "load_balance";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    // The exit handling strategy is a bit of a mess here. We potentially have
    // three sources of exit messages:
    // 1) Spawning the actor with `caf::linked`, which is bidirectional. Exit
    //    messages are exchanges when the load balancer dies or after the
    //    execution node has terminated. This is also used to exit execution if
    //    all subpipelines have terminated, for example when they use `head`.
    // 2) Wrapping the actor with `scope_linked`, which sends an exit message at
    //    the end of the scope. This can thus happen before the previous one and
    //    ensures that we still have access to all resources. It is also called
    //    when we destroy the generator from the outside.
    // 3) At the end of the generator, an explicit exit messages is sent we wait
    //    until the actor terminates. This is important because we only want to
    //    return from the generator (which signals completion) when all
    //    subpipelines are fully completed.
    // In case of subtle problems around the shutdown logic here, this could
    // potentially be simplified.
    auto load_balancer = scope_linked{ctrl.self().spawn<caf::linked>(
      make_load_balancer, pipes_, std::string{ctrl.definition()},
      ctrl.shared_diagnostics(), ctrl.metrics_receiver(), ctrl.operator_index(),
      ctrl.is_hidden(), ctrl.node(), std::string{ctrl.pipeline_id()})};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::write_v, std::move(slice))
        .request(load_balancer.get(), caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            // This should never happen.
            diagnostic::error("failed to write data to load balancer")
              .note("reason: {}", err)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    TENZIR_DEBUG("waiting for termination of load_balancer");
    ctrl.set_waiting(true);
    load_balancer.get()->attach_functor(
      [self = caf::actor_cast<caf::actor>(&ctrl.self()), &ctrl] {
        caf::anon_mail(caf::make_action([&ctrl] {
          ctrl.set_waiting(false);
        }))
          .send(self);
      });
    caf::anon_send_exit(load_balancer.get(), caf::exit_reason::user_shutdown);
    co_yield {};
    TENZIR_DEBUG("load_balance terminated");
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, load_balance& x) -> bool {
    return f.apply(x.pipes_);
  }

private:
  std::vector<pipeline> pipes_;
};

class plugin final : public virtual operator_plugin2<load_balance>,
                     public virtual operator_compiler_plugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipes = std::vector<pipeline>{};
    for (auto& arg : inv.args) {
      auto pipe = std::get_if<ast::pipeline_expr>(&*arg.kind);
      TENZIR_ASSERT(pipe);
      TRY(auto compiled, tenzir::compile(std::move(pipe->inner), ctx));
      auto output = compiled.infer_type<table_slice>();
      if (not output) {
        diagnostic::error("pipeline must take events as input")
          .primary(pipe->begin)
          .emit(ctx);
        return failure::promise();
      }
      if (not output->is<void>()) {
        diagnostic::error("pipeline must currently end with a sink")
          .primary(pipe->end)
          .emit(ctx);
        return failure::promise();
      }
      pipes.push_back(std::move(compiled));
    }
    return std::make_unique<load_balance>(std::move(pipes));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto const* docs = "https://docs.tenzir.com/tql2/operators/load_balance";
    auto const* usage = "load_balance over:list { … }";
    auto emit = [&](diagnostic_builder d) {
      std::move(d).docs(docs).usage(usage).emit(ctx);
    };
    if (inv.args.empty()) {
      emit(diagnostic::error("expected two positional arguments")
             .primary(inv.op));
      return failure::promise();
    }
    auto* var = try_as<ast::dollar_var>(inv.args[0]);
    if (not var) {
      emit(diagnostic::error("expected a `$`-variable").primary(inv.args[0]));
      return failure::promise();
    }
    TRY(inv.args[0].bind(ctx));
    var = try_as<ast::dollar_var>(inv.args[0]);
    TENZIR_ASSERT(var);
    if (inv.args.size() < 2) {
      emit(diagnostic::error("expected a pipeline afterwards").primary(*var));
      return failure::promise();
    }
    auto* pipe = try_as<ast::pipeline_expr>(inv.args[1]);
    if (not pipe) {
      emit(diagnostic::error("expected a pipeline expression")
             .primary(inv.args[1]));
      return failure::promise();
    }
    if (inv.args.size() > 2) {
      emit(diagnostic::error("expected exactly two arguments, got {}",
                             inv.args.size())
             .primary(inv.args[2]));
      return failure::promise();
    }
    auto self = inv.op.get_location();
    auto pipe_location = pipe->get_location();
    TRY(auto pipe_ir, std::move(pipe->inner).compile(ctx));
    return LoadBalanceIr{self, *var, pipe_location, std::move(pipe_ir)};
  }
};

using load_balance_ir_plugin
  = inspection_plugin<ir::Operator, LoadBalanceIr>;

} // namespace

} // namespace tenzir::plugins::load_balance

TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_balance::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_balance::load_balance_ir_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::operator_inspection_plugin<
                       tenzir::plugins::load_balance::load_balance_source>)
