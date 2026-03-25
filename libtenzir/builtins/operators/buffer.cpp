//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/async/mutex.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/detail/enum.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>
#include <tenzir/uuid.hpp>

#include <caf/actor_registry.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <optional>
#include <queue>

namespace tenzir::plugins::buffer {

// TODO: gcc emits a bogus -Wunused-function warning for this macro when used
// inside an anonymous namespace.
TENZIR_ENUM(buffer_policy, block, drop);

namespace {

template <class Elements>
using buffer_actor = caf::typed_actor<
  // Write elements into the buffer.
  auto(atom::write, Elements elements)->caf::result<void>,
  // Read elements from the buffer.
  auto(atom::read)->caf::result<Elements>>;

template <class Elements>
struct buffer_state {
  static constexpr auto name = "buffer";

  buffer_actor<Elements>::pointer self = {};
  located<uint64_t> capacity = {};
  buffer_policy policy = {};
  metric_handler metrics_handler = {};
  shared_diagnostic_handler diagnostics_handler = {};

  uint64_t buffer_size = {};
  std::queue<Elements> buffer = {};
  caf::typed_response_promise<Elements> read_rp = {};

  Elements blocked_elements = {};
  caf::typed_response_promise<void> write_rp = {};

  uint64_t num_dropped = {};

  ~buffer_state() noexcept {
    emit_metrics();
    if (read_rp.pending()) {
      read_rp.deliver(Elements{});
    }
  }

  auto write(Elements elements) -> caf::result<void> {
    if (read_rp.pending()) {
      read_rp.deliver(std::move(elements));
      return {};
    }
    if (buffer_size + size(elements) > capacity.inner) {
      auto [lhs, rhs] = split(elements, capacity.inner - buffer_size);
      if (const auto lhs_size = size(lhs); lhs_size > 0) {
        buffer_size += lhs_size;
        buffer.push(std::move(lhs));
      }
      TENZIR_ASSERT(size(rhs) > 0);
      switch (policy) {
        case buffer_policy::drop: {
          num_dropped += size(rhs);
          diagnostic::warning("buffer exceeded capacity and dropped elements")
            .primary(capacity.source)
            .hint("the configured policy is `{}`; use `{}` to prevent dropping",
                  buffer_policy::drop, buffer_policy::block)
            .note("the `metrics` operator allows for monitoring buffers")
            .emit(diagnostics_handler);
          return {};
        }
        case buffer_policy::block: {
          TENZIR_ASSERT(size(blocked_elements) == 0);
          TENZIR_ASSERT(not write_rp.pending());
          blocked_elements = std::move(rhs);
          write_rp = self->template make_response_promise<void>();
          return write_rp;
        }
      }
    }
    buffer_size += size(elements);
    buffer.push(std::move(elements));
    return {};
  }

  auto read() -> caf::result<Elements> {
    TENZIR_ASSERT(not read_rp.pending());
    if (not buffer.empty()) {
      auto elements = std::move(buffer.front());
      buffer_size -= size(elements);
      buffer.pop();
      if (write_rp.pending()) {
        TENZIR_ASSERT(policy == buffer_policy::block);
        const auto free_capacity = capacity.inner - buffer_size;
        auto [lhs, rhs] = split(blocked_elements, free_capacity);
        if (const auto lhs_size = size(lhs); lhs_size > 0) {
          buffer_size += lhs_size;
          buffer.push(std::move(lhs));
        }
        blocked_elements = std::move(rhs);
        if (size(blocked_elements) == 0) {
          write_rp.deliver();
        }
      }
      return elements;
    }
    read_rp = self->template make_response_promise<Elements>();
    return read_rp;
  }

  auto emit_metrics() -> void {
    TENZIR_ASSERT(capacity.inner >= buffer_size);
    metrics_handler.emit({
      {"used", buffer_size},
      {"free", capacity.inner - buffer_size},
      {"dropped", std::exchange(num_dropped, {})},
    });
  }
};

template <class Elements>
auto make_buffer(typename buffer_actor<Elements>::template stateful_pointer<
                   buffer_state<Elements>>
                   self,
                 located<uint64_t> capacity, buffer_policy policy,
                 metric_handler metrics_handler,
                 shared_diagnostic_handler diagnostics_handler)
  -> buffer_actor<Elements>::behavior_type {
  self->state().self = self;
  self->state().capacity = capacity;
  self->state().policy = policy;
  self->state().metrics_handler = std::move(metrics_handler);
  self->state().diagnostics_handler = std::move(diagnostics_handler);
  detail::weak_run_delayed_loop(self, defaults::metrics_interval, [self] {
    self->state().emit_metrics();
  });
  return {
    [self](atom::write, Elements& elements) -> caf::result<void> {
      return self->state().write(std::move(elements));
    },
    [self](atom::read) -> caf::result<Elements> {
      return self->state().read();
    },
    [self](caf::exit_msg& msg) {
      // The buffer actor is linked to both internal operators. We want to
      // unconditionally shut down the buffer actor, even when the operator
      // shuts down without an error.
      self->quit(std::move(msg.reason));
    },
  };
}

struct BufferArgs {
  located<uint64_t> capacity;
  std::optional<located<std::string>> policy_str;
};

template <class T>
  requires(detail::is_any_v<T, table_slice, chunk_ptr>)
class Buffer final : public Operator<T, T> {
public:
  explicit Buffer(BufferArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if constexpr (std::is_same_v<T, chunk_ptr>) {
      // drop policy is rejected at compile time by the spawner in describe();
      // bytes buffers always block.
      policy_ = buffer_policy::block;
    } else {
      if (args_.policy_str) {
        const auto parsed = from_string<buffer_policy>(args_.policy_str->inner);
        TENZIR_ASSERT(parsed); // validated in describe()
        policy_ = *parsed;
      } else {
        policy_ = ctx.is_hidden() ? buffer_policy::drop : buffer_policy::block;
      }
    }
    // TODO: implement custom metrics for the new executor
    // We don't want to use metric_handler, because that requires
    // driving a clock for emitting events, which we don't want to do in the
    // operator itself. Instead, we'd want to have a counters and gauges
    // registered in context that can be updated cheaply and whose values are
    // periodically collected by the executor.
    //
    // metric_dropped_ = ctx.make_counter(...);
    // metric_free_ = ctx.make_gauge(...);
    // metric_used_ = ctx.make_gauge(...);
    co_return;
  }

  auto process(T input, Push<T>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(push);
    if (size(input) == 0) {
      co_return;
    }
    if (policy_ == buffer_policy::drop) {
      auto guard = co_await state_->lock();
      const auto free = args_.capacity.inner - guard->size;
      if (free == 0) {
        guard.unlock();
        diagnostic::warning("buffer exceeded capacity and dropped elements")
          .primary(args_.capacity.source)
          .hint("the configured policy is `{}`; use `{}` to prevent dropping",
                buffer_policy::drop, buffer_policy::block)
          .note("the `metrics` operator allows for monitoring buffers")
          .emit(ctx.dh());
        co_return;
      }
      auto [lhs, rhs] = split(input, free);
      guard->size += size(lhs);
      guard->queue.push(std::move(lhs));
      guard.unlock();
      data_available_->notify_one();
      if (size(rhs) > 0) {
        diagnostic::warning("buffer exceeded capacity and dropped elements")
          .primary(args_.capacity.source)
          .hint("the configured policy is `{}`; use `{}` to prevent dropping",
                buffer_policy::drop, buffer_policy::block)
          .note("the `metrics` operator allows for monitoring buffers")
          .emit(ctx.dh());
      }
    } else {
      // block policy: enqueue what fits, wait for space when full
      while (size(input) > 0) {
        {
          auto guard = co_await state_->lock();
          const auto free = args_.capacity.inner - guard->size;
          if (free > 0) {
            auto [lhs, rhs] = split(input, free);
            guard->size += size(lhs);
            guard->queue.push(std::move(lhs));
            input = std::move(rhs);
            guard.unlock();
            data_available_->notify_one();
          }
        }
        if (size(input) > 0) {
          co_await space_available_->wait();
        }
      }
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    while (true) {
      {
        auto guard = co_await state_->lock();
        if (not guard->queue.empty()) {
          auto item = std::move(guard->queue.front());
          guard->queue.pop();
          guard->size -= size(item);
          guard.unlock();
          space_available_->notify_one();
          co_return Option{std::move(item)};
        }
        if (guard->closed) {
          co_return Option<T>{None{}};
        }
      }
      co_await data_available_->wait();
    }
  }

  auto process_task(Any result, Push<T>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto* opt = result.try_as<Option<T>>();
    if (not opt) {
      co_return;
    }
    if (not *opt) {
      done_ = true;
      co_return;
    }
    co_await push(std::move(**opt));
  }

  auto prepare_snapshot(Push<T>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);

    while (true) {
      Option<T> item;
      {
        auto guard = co_await state_->lock();
        if (guard->queue.empty()) {
          co_return;
        }
        item = std::move(guard->queue.front());
        guard->queue.pop();
        guard->size -= size(*item);
      }
      space_available_->notify_one();
      co_await push(std::move(*item));
    }
  }

  auto finalize(Push<T>& push, OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    {
      auto guard = co_await state_->lock();
      guard->closed = true;
    }
    data_available_->notify_one();
    co_return FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // Buffer contents are flushed downstream in prepare_snapshot before the
    // snapshot is taken, so there is nothing to serialize here.
  }

private:
  struct InternalState {
    std::queue<T> queue = {};
    uint64_t size = 0;
    bool closed = false;
  };

  BufferArgs args_;
  buffer_policy policy_ = buffer_policy::block;

  mutable Box<Mutex<InternalState>> state_{std::in_place, InternalState{}};
  mutable Box<Notify> data_available_{std::in_place};
  mutable Box<Notify> space_available_{std::in_place};
  bool done_ = false;
};

class write_buffer_operator final
  : public crtp_operator<write_buffer_operator> {
public:
  write_buffer_operator() = default;

  explicit write_buffer_operator(uuid id) : id_{id} {
  }

  template <class Elements>
    requires(detail::is_any_v<Elements, table_slice, chunk_ptr>)
  auto operator()(generator<Elements> input, operator_control_plane& ctrl) const
    -> generator<Elements> {
    // The internal-write-buffer operator is spawned after the
    // internal-read-buffer operator, so we can safely get the buffer actor here
    // after the first yield and then just remove it from the registry again.
    co_yield {};
    auto buffer = ctrl.self().system().registry().get<buffer_actor<Elements>>(
      fmt::format("tenzir.buffer.{}.{}", id_, ctrl.run_id()));
    TENZIR_ASSERT(buffer);
    ctrl.self().link_to(buffer);
    ctrl.self().system().registry().erase(buffer->id());
    // Now, all we need to do is send our inputs to the buffer batch by batch.
    for (auto&& elements : input) {
      if (size(elements) == 0) {
        co_yield {};
        continue;
      }
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::write_v, std::move(elements))
        .request(buffer, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](caf::error& err) {
            diagnostic::error(err)
              .note("failed to write to buffer")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "internal-write-buffer";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, copy()};
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<table_slice>()) {
      return tag_v<table_slice>;
    }
    if (input.is<chunk_ptr>()) {
      return tag_v<chunk_ptr>;
    }
    return diagnostic::error("`buffer` does not accept {} as input",
                             operator_type_name(input))
      .to_error();
  }

  friend auto inspect(auto& f, write_buffer_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_));
  }

private:
  uuid id_ = {};
};

class read_buffer_operator final : public crtp_operator<read_buffer_operator> {
public:
  read_buffer_operator() = default;

  explicit read_buffer_operator(uuid id, located<uint64_t> capacity,
                                std::optional<located<buffer_policy>> policy)
    : id_{id}, capacity_{capacity}, policy_{policy} {
  }

  template <class Elements>
  auto policy(operator_control_plane& ctrl) const -> buffer_policy {
    if (std::is_same_v<Elements, table_slice>) {
      if (policy_) {
        return policy_->inner;
      }
      return ctrl.is_hidden() ? buffer_policy::drop : buffer_policy::block;
    }
    if (policy_ and policy_->inner == buffer_policy::drop) {
      diagnostic::error("`drop` policy is unsupported for bytes inputs")
        .note("use `block` instead")
        .primary(policy_->source)
        .emit(ctrl.diagnostics());
    }
    return buffer_policy::block;
  }

  static auto metrics(operator_control_plane& ctrl) -> metric_handler {
    return ctrl.metrics(type{
      "tenzir.metrics.buffer",
      record_type{
        {"used", uint64_type{}},
        {"free", uint64_type{}},
        {"dropped", uint64_type{}},
      },
    });
  }

  template <class Elements>
    requires(detail::is_any_v<Elements, table_slice, chunk_ptr>)
  auto operator()(generator<Elements> input, operator_control_plane& ctrl) const
    -> generator<Elements> {
    // The internal-read-buffer operator is spawned before the
    // internal-write-buffer operator, so we spawn the buffer actor here and
    // move it into the registry before the first yield.
    auto buffer
      = ctrl.self().spawn<caf::linked>(make_buffer<Elements>, capacity_,
                                       policy<Elements>(ctrl), metrics(ctrl),
                                       ctrl.shared_diagnostics());
    ctrl.self().system().registry().put(
      fmt::format("tenzir.buffer.{}.{}", id_, ctrl.run_id()), buffer);
    co_yield {};
    // Now, we can get batch by batch from the buffer.
    for (auto&& elements : input) {
      TENZIR_ASSERT(size(elements) == 0);
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::read_v)
        .request(buffer, caf::infinite)
        .then(
          [&](Elements& response) {
            ctrl.set_waiting(false);
            elements = std::move(response);
          },
          [&](caf::error& err) {
            diagnostic::error(err)
              .note("failed to read from buffer")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      co_yield std::move(elements);
    }
  }

  auto name() const -> std::string override {
    return "internal-read-buffer";
  }

  auto idle_after() const -> duration override {
    // We only send stub elements between the two operators to break the back
    // pressure and instead use a side channel for transporting elements, hence
    // the nead to schedule the reading side independently of receiving input.
    return duration::max();
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, copy()};
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<table_slice>()) {
      return tag_v<table_slice>;
    }
    if (input.is<chunk_ptr>()) {
      return tag_v<chunk_ptr>;
    }
    return diagnostic::error("`buffer` does not accept {} as input",
                             operator_type_name(input))
      .to_error();
  }

  friend auto inspect(auto& f, read_buffer_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_),
                              f.field("capacity", x.capacity_),
                              f.field("policy", x.policy_));
  }

private:
  uuid id_ = {};
  located<uint64_t> capacity_ = {};
  std::optional<located<buffer_policy>> policy_ = {};
};

class buffer_plugin final : public virtual operator_parser_plugin,
                            public virtual operator_factory_plugin,
                            public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "buffer";
  };

  auto signature() const -> operator_signature override {
    return {
      .source = false,
      .transformation = true,
      .sink = false,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"buffer", "https://docs.tenzir.com/"
                                            "operators/buffer"};
    auto capacity = located<uint64_t>{};
    auto policy_str = std::optional<located<std::string>>{};
    parser.add(capacity, "<capacity>");
    parser.add("--policy", policy_str, "<block|drop>");
    parser.parse(p);
    if (capacity.inner == 0) {
      // TODO: It'd be nice to underline the entire operator's definition here
      // instead of just the capacity, but there is no easy way to get that
      // location currently.
      diagnostic::error("capacity must be greater than zero")
        .primary(capacity.source)
        .throw_();
    }
    auto policy = std::optional<located<buffer_policy>>{};
    if (policy_str) {
      const auto parsed_policy = from_string<buffer_policy>(policy_str->inner);
      if (not parsed_policy) {
        diagnostic::error("policy must be 'block' or 'drop'")
          .primary(policy_str->source)
          .throw_();
      }
      policy = {*parsed_policy, policy_str->source};
    }
    const auto id = uuid::random();
    auto result = std::make_unique<pipeline>();
    result->append(std::make_unique<write_buffer_operator>(id));
    result->append(
      std::make_unique<read_buffer_operator>(id, capacity, policy));
    return result;
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto capacity = located<uint64_t>{};
    auto policy_str = std::optional<located<std::string>>{};
    argument_parser2::operator_("buffer")
      .positional("capacity", capacity)
      .named("policy", policy_str)
      .parse(inv, ctx)
      .ignore();
    auto failed = false;
    if (capacity.inner == 0) {
      diagnostic::error("capacity must be greater than zero")
        .primary(capacity.source)
        .emit(ctx);
      failed = true;
    }
    auto policy = std::optional<located<buffer_policy>>{};
    if (policy_str) {
      const auto parsed_policy = from_string<buffer_policy>(policy_str->inner);
      if (not parsed_policy) {
        diagnostic::error("policy must be 'block' or 'drop'")
          .primary(policy_str->source)
          .emit(ctx);
        failed = true;
      }
      policy = {*parsed_policy, policy_str->source};
    }
    if (failed) {
      return failure::promise();
    }
    const auto id = uuid::random();
    auto result = std::make_unique<pipeline>();
    result->append(std::make_unique<write_buffer_operator>(id));
    result->append(
      std::make_unique<read_buffer_operator>(id, capacity, policy));
    return result;
  }

  auto describe() const -> Description override {
    auto d = Describer<BufferArgs>{};
    auto capacity_arg = d.positional("capacity", &BufferArgs::capacity);
    auto policy_arg = d.named("policy", &BufferArgs::policy_str, "block|drop");
    d.validate([capacity_arg, policy_arg](DescribeCtx& ctx) -> Empty {
      TRY(auto cap, ctx.get(capacity_arg));
      if (cap.inner == 0) {
        diagnostic::error("capacity must be greater than zero")
          .primary(cap.source)
          .emit(ctx);
      }
      if (auto pol = ctx.get(policy_arg)) {
        if (not from_string<buffer_policy>(pol->inner)) {
          diagnostic::error("policy must be 'block' or 'drop'")
            .primary(pol->source)
            .emit(ctx);
        }
      }
      return {};
    });
    d.spawner([policy_arg]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<BufferArgs, Input>>> {
      if constexpr (std::same_as<Input, table_slice>) {
        return [](BufferArgs args) {
          return Buffer<table_slice>{std::move(args)};
        };
      } else if constexpr (std::same_as<Input, chunk_ptr>) {
        if (auto pol = ctx.get(policy_arg)) {
          if (from_string<buffer_policy>(pol->inner) == buffer_policy::drop) {
            diagnostic::error("`drop` policy is unsupported for bytes inputs")
              .note("use `block` instead")
              .primary(pol->source)
              .emit(ctx);
            return failure::promise();
          }
        }
        return [](BufferArgs args) {
          return Buffer<chunk_ptr>{std::move(args)};
        };
      } else {
        return {};
      }
    });
    return d.without_optimize();
  }
};

using write_buffer_plugin = operator_inspection_plugin<write_buffer_operator>;
using read_buffer_plugin = operator_inspection_plugin<read_buffer_operator>;

} // namespace

} // namespace tenzir::plugins::buffer

TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::buffer_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::write_buffer_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::read_buffer_plugin)
