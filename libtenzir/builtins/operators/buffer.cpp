//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/enum.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace tenzir::plugins::buffer {

namespace {

using buffer_actor = caf::typed_actor<
  // Write events into the buffer.
  auto(atom::write, table_slice events)->caf::result<void>,
  // Read events from the buffer.
  auto(atom::read)->caf::result<table_slice>>;

TENZIR_ENUM(buffer_policy, block, drop);

struct buffer_state {
  static constexpr auto name = "buffer";

  buffer_actor::pointer self = {};
  located<uint64_t> capacity = {};
  buffer_policy policy = {};
  metric_handler metrics_handler = {};
  shared_diagnostic_handler diagnostics_handler = {};

  uint64_t buffer_size = {};
  std::queue<table_slice> buffer = {};
  caf::typed_response_promise<table_slice> read_rp = {};

  table_slice blocked_events = {};
  caf::typed_response_promise<void> write_rp = {};

  uint64_t num_dropped = {};

  ~buffer_state() noexcept {
    emit_metrics();
    if (read_rp.pending()) {
      read_rp.deliver(table_slice{});
    }
  }

  auto write(table_slice events) -> caf::result<void> {
    if (read_rp.pending()) {
      read_rp.deliver(std::move(events));
      return {};
    }
    if (buffer_size + events.rows() > capacity.inner) {
      auto [lhs, rhs] = split(events, capacity.inner - buffer_size);
      buffer_size += lhs.rows();
      buffer.push(std::move(lhs));
      TENZIR_ASSERT(rhs.rows() > 0);
      switch (policy) {
        case buffer_policy::drop: {
          num_dropped += rhs.rows();
          diagnostic::warning("buffer exceeded capacity and dropped events")
            .primary(capacity.source)
            .hint("the configured policy is `{}`; use `{}` to prevent dropping",
                  buffer_policy::drop, buffer_policy::block)
            .note("the `metrics` operator allows for monitoring buffers")
            .emit(diagnostics_handler);
          return {};
        }
        case buffer_policy::block: {
          TENZIR_ASSERT(blocked_events.rows() == 0);
          TENZIR_ASSERT(not write_rp.pending());
          blocked_events = std::move(rhs);
          write_rp = self->make_response_promise<void>();
          return write_rp;
        }
      }
    }
    buffer_size += events.rows();
    buffer.push(std::move(events));
    return {};
  }

  auto read() -> caf::result<table_slice> {
    TENZIR_ASSERT(not read_rp.pending());
    if (not buffer.empty()) {
      auto events = std::move(buffer.front());
      buffer_size -= events.rows();
      buffer.pop();
      if (write_rp.pending()) {
        TENZIR_ASSERT(policy == buffer_policy::block);
        const auto free_capacity = capacity.inner - buffer_size;
        auto [lhs, rhs] = split(blocked_events, free_capacity);
        buffer_size += lhs.rows();
        buffer.push(std::move(lhs));
        blocked_events = std::move(rhs);
        if (blocked_events.rows() == 0) {
          write_rp.deliver();
        }
      }
      return events;
    }
    read_rp = self->make_response_promise<table_slice>();
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

auto make_buffer(buffer_actor::stateful_pointer<buffer_state> self,
                 located<uint64_t> capacity, buffer_policy policy,
                 metric_handler metrics_handler,
                 shared_diagnostic_handler diagnostics_handler)
  -> buffer_actor::behavior_type {
  self->state.self = self;
  self->state.capacity = capacity;
  self->state.policy = policy;
  self->state.metrics_handler = std::move(metrics_handler);
  self->state.diagnostics_handler = std::move(diagnostics_handler);
  self->set_exit_handler([self](caf::exit_msg& msg) {
    // The buffer actor is linked to both internal operators. We want to
    // unconditionally shut down the buffer actor, even when the operator shuts
    // down without an error.
    self->quit(std::move(msg.reason));
  });
  detail::weak_run_delayed_loop(self, defaults::metrics_interval, [self] {
    self->state.emit_metrics();
  });
  return {
    [self](atom::write, table_slice& events) -> caf::result<void> {
      return self->state.write(std::move(events));
    },
    [self](atom::read) -> caf::result<table_slice> {
      return self->state.read();
    },
  };
}

class write_buffer_operator final
  : public crtp_operator<write_buffer_operator> {
public:
  write_buffer_operator() = default;

  explicit write_buffer_operator(uuid id) : id_{id} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // The internal-write-buffer operator is spawned after the
    // internal-read-buffer operator, so we can safely get the buffer actor here
    // after the first yield and then just remove it from the registry again.
    co_yield {};
    auto buffer = ctrl.self().system().registry().get<buffer_actor>(
      fmt::format("tenzir.buffer.{}", id_));
    TENZIR_ASSERT(buffer);
    ctrl.self().link_to(buffer);
    ctrl.self().system().registry().erase(buffer->id());
    // Now, all we need to do is send our inputs to the buffer batch by batch.
    for (auto&& events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.set_waiting(true);
      ctrl.self()
        .request(buffer, caf::infinite, atom::write_v, std::move(events))
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
                                std::optional<buffer_policy> policy)
    : id_{id}, capacity_{capacity}, policy_{policy} {
  }

  auto policy(operator_control_plane& ctrl) const -> buffer_policy {
    return policy_.value_or(ctrl.is_hidden() ? buffer_policy::drop
                                             : buffer_policy::block);
  }

  auto metrics(operator_control_plane& ctrl) const -> metric_handler {
    return ctrl.metrics(type{
      "tenzir.metrics.buffer",
      record_type{
        {"used", uint64_type{}},
        {"remaining", uint64_type{}},
        {"dropped", uint64_type{}},
      },
    });
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // The internal-read-buffer operator is spawned before the
    // internal-write-buffer operator, so we spawn the buffer actor here and
    // move it into the registry before the first yield.
    auto buffer = ctrl.self().spawn<caf::linked>(make_buffer, capacity_,
                                                 policy(ctrl), metrics(ctrl),
                                                 ctrl.shared_diagnostics());
    ctrl.self().system().registry().put(fmt::format("tenzir.buffer.{}", id_),
                                        buffer);
    co_yield {};
    // Now, we can get batch by batch from the buffer.
    for (auto&& events : input) {
      TENZIR_ASSERT(events.rows() == 0);
      ctrl.set_waiting(true);
      ctrl.self()
        .request(buffer, caf::infinite, atom::read_v)
        .then(
          [&](table_slice& response) {
            ctrl.set_waiting(false);
            events = std::move(response);
          },
          [&](caf::error& err) {
            diagnostic::error(err)
              .note("failed to read from buffer")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      if (events.rows() == 0) {
        co_return;
      }
      co_yield std::move(events);
    }
  }

  auto name() const -> std::string override {
    return "internal-read-buffer";
  }

  auto input_independent() const -> bool override {
    // We only send stub events between the two operators to break the back
    // pressure and instead use a side channel for transporting events, hence
    // the nead to schedule the reading side independently of receiving input.
    return true;
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
  std::optional<buffer_policy> policy_ = {};
};

class buffer_plugin final : public virtual operator_parser_plugin,
                            public virtual operator_factory_plugin {
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
      diagnostic::error("capacity must be greater than zero")
        .primary(capacity.source)
        .throw_();
    }
    auto policy = std::optional<buffer_policy>{};
    if (policy_str) {
      policy = from_string<buffer_policy>(policy_str->inner);
      if (not policy) {
        diagnostic::error("policy must be 'block' or 'drop'")
          .primary(policy_str->source)
          .throw_();
      }
    }
    const auto id = uuid::random();
    auto result = std::make_unique<pipeline>();
    result->append(std::make_unique<write_buffer_operator>(id));
    result->append(
      std::make_unique<read_buffer_operator>(id, capacity, policy));
    return result;
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto capacity = located<uint64_t>{};
    auto policy_str = std::optional<located<std::string>>{};
    argument_parser2::operator_("buffer")
      .add(capacity, "<capacity>")
      .add("policy", policy_str)
      .parse(inv, ctx)
      .ignore();
    if (capacity.inner == 0) {
      diagnostic::error("capacity must be greater than zero")
        .primary(capacity.source)
        .emit(ctx);
    }
    auto policy = std::optional<buffer_policy>{};
    if (policy_str) {
      policy = from_string<buffer_policy>(policy_str->inner);
      if (not policy) {
        diagnostic::error("policy must be 'block' or 'drop'")
          .primary(policy_str->source)
          .emit(ctx);
      }
    }
    const auto id = uuid::random();
    auto result = std::make_unique<pipeline>();
    result->append(std::make_unique<write_buffer_operator>(id));
    result->append(
      std::make_unique<read_buffer_operator>(id, capacity, policy));
    return result;
  }
};

using write_buffer_plugin = operator_inspection_plugin<write_buffer_operator>;
using read_buffer_plugin = operator_inspection_plugin<read_buffer_operator>;

} // namespace

} // namespace tenzir::plugins::buffer

TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::buffer_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::write_buffer_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::read_buffer_plugin)
