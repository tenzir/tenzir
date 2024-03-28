//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/type.h>
#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace tenzir::plugins::accept {

namespace {

using bridge_actor = caf::typed_actor<
  // Forwards slices from the connection actors to the operator
  auto(table_slice slice)->caf::result<void>,
  auto(atom::get)->caf::result<table_slice>>;

struct bridge_state {
  std::queue<table_slice> buffer;
  caf::typed_response_promise<table_slice> buffer_rp;
};

auto make_bridge(bridge_actor::stateful_pointer<bridge_state> self)
  -> bridge_actor::behavior_type {
  return {
    [self](table_slice& slice) -> caf::result<void> {
      if (self->state.buffer_rp.pending()) {
        TENZIR_ASSERT(self->state.buffer.empty());
        self->state.buffer_rp.deliver(std::move(slice));
        return {};
      }
      self->state.buffer.push(std::move(slice));
      return {};
    },
    [self](atom::get) -> caf::result<table_slice> {
      TENZIR_ASSERT(not self->state.buffer_rp.pending());
      if (self->state.buffer.empty()) {
        self->state.buffer_rp = self->make_response_promise<table_slice>();
        return self->state.buffer_rp;
      }
      auto ts = std::move(self->state.buffer.front());
      self->state.buffer.pop();
      return ts;
    },
  };
}

struct connection_manager_state {};

auto make_connection_manager(caf::stateful_actor<connection_manager_state>* self,
                             bridge_actor bridge) -> caf::behavior {
  return {};
}

struct connection_state {};

auto make_connection_actor(caf::stateful_actor<connection_state>* self,
                           bridge_actor bridge,
                           boost::asio::ip::tcp::socket socket, bool use_tls)
  -> caf::behavior {
  return {};
}

class accept_operator final : public operator_base {
public:
  accept_operator() = default;

  accept_operator(operator_ptr op, duration interval)
    : op_{std::move(op)}, interval_{interval} {
    if (auto* op = dynamic_cast<accept_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    TENZIR_ASSERT(not dynamic_cast<const accept_operator*>(op_.get()));
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    auto result = op_->optimize(filter, order);
    if (not result.replacement) {
      return result;
    }
    if (auto* pipe = dynamic_cast<pipeline*>(result.replacement.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<accept_operator>(std::move(result.replacement),
                                               interval_);
        // Only the first operator can be a source and needs to be replaced.
        break;
      }
      result.replacement = std::make_unique<pipeline>(std::move(ops));
      return result;
    }
    result.replacement = std::make_unique<accept_operator>(
      std::move(result.replacement), interval_);
    return result;
  }

  template <class T>
  static auto run(operator_ptr op, duration interval, operator_input input,
                  operator_control_plane& ctrl) -> generator<T> {
    TENZIR_ASSERT(std::holds_alternative<std::monostate>(input));
    auto alarm_clock = ctrl.self().spawn(make_alarm_clock);
    auto next_run = time::clock::now() + interval;
    co_yield {};
    while (true) {
      auto gen = op->instantiate(std::monostate{}, ctrl);
      if (not gen) {
        diagnostic::error(gen.error()).emit(ctrl.diagnostics());
        co_return;
      }
      auto typed_gen = std::get_if<generator<T>>(&*gen);
      TENZIR_ASSERT(typed_gen);
      for (auto&& result : *typed_gen) {
        co_yield std::move(result);
      }
      const auto now = time::clock::now();
      const auto delta = next_run - now;
      if (delta < duration::zero()) {
        next_run = now + interval;
        continue;
      }
      next_run += interval;
      ctrl.self()
        .request(alarm_clock, caf::infinite, delta)
        .await([]() { /*nop*/ },
               [&](const caf::error& err) {
                 diagnostic::error(err)
                   .note("failed to wait for {} timeout", data{interval})
                   .emit(ctrl.diagnostics());
               });
      co_yield {};
    }
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    auto output = infer_type<void>();
    TENZIR_ASSERT(output);
    TENZIR_ASSERT(output->is_not<void>());
    if (output->is<table_slice>()) {
      return run<table_slice>(op_->copy(), interval_, std::move(input), ctrl);
    }
    TENZIR_ASSERT(output->is<chunk_ptr>());
    return run<chunk_ptr>(op_->copy(), interval_, std::move(input), ctrl);
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<accept_operator>(op_->copy(), interval_);
  };

  auto location() const -> operator_location override {
    return op_->location();
  }

  auto detached() const -> bool override {
    return op_->detached();
  }

  auto internal() const -> bool override {
    return op_->internal();
  }

  auto input_independent() const -> bool override {
    return op_->input_independent();
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (not input.is<void>()) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("`{}` must be used with a source operator", name()));
    }
    return op_->infer_type(input);
  }

  auto name() const -> std::string override {
    return "accept";
  }

  friend auto inspect(auto& f, accept_operator& x) -> bool {
    return f.object(x).fields(f.field("op", x.op_),
                              f.field("interval", x.interval_));
  }

private:
  operator_ptr op_;
  duration interval_;
};

class accept_plugin final : public virtual operator_plugin<accept_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto interval_data = p.parse_data();
    const auto* interval = caf::get_if<duration>(&interval_data.inner);
    if (not interval) {
      diagnostic::error("interval must be a duration")
        .primary(interval_data.source)
        .throw_();
    }
    if (*interval <= duration::zero()) {
      diagnostic::error("interval must be a positive duration")
        .primary(interval_data.source)
        .throw_();
    }
    auto op_name = p.accept_identifier();
    if (!op_name) {
      diagnostic::error("expected operator name")
        .primary(p.current_span())
        .throw_();
    }
    const auto* plugin = plugins::find_operator(op_name->name);
    if (!plugin) {
      diagnostic::error("operator `{}` does not exist", op_name->name)
        .primary(op_name->source)
        .throw_();
    }
    auto result = plugin->parse_operator(p);
    if (auto* pipe = dynamic_cast<pipeline*>(result.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<accept_operator>(std::move(op), *interval);
        // Only the first operator can be a source and needs to be replaced.
        break;
      }
      return std::make_unique<pipeline>(std::move(ops));
    }
    return std::make_unique<accept_operator>(std::move(result), *interval);
  }
};

} // namespace

} // namespace tenzir::plugins::accept

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept::accept_plugin)
