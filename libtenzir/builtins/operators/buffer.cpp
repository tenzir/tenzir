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
    [self](atom::write, Elements& elements) -> caf::result<void> {
      return self->state.write(std::move(elements));
    },
    [self](atom::read) -> caf::result<Elements> {
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
        .request(buffer, caf::infinite, atom::write_v, std::move(elements))
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
        .request(buffer, caf::infinite, atom::read_v)
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

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto capacity = located<uint64_t>{};
    auto policy_str = std::optional<located<std::string>>{};
    argument_parser2::operator_("buffer")
      .add(capacity, "<capacity>")
      .add("policy", policy_str)
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
};

using write_buffer_plugin = operator_inspection_plugin<write_buffer_operator>;
using read_buffer_plugin = operator_inspection_plugin<read_buffer_operator>;

} // namespace

} // namespace tenzir::plugins::buffer

TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::buffer_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::write_buffer_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::buffer::read_buffer_plugin)
