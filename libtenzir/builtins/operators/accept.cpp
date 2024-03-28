//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace tenzir::plugins::accept {

namespace {

using bridge_actor = caf::typed_actor<
  // Forwards slices from the connection actors to the operator
  auto(table_slice slice)->caf::result<void>,
  auto(atom::get)->caf::result<table_slice>>;

struct connection_state {};

auto make_connection(caf::stateful_actor<connection_state>* self,
                     bridge_actor bridge, boost::asio::ip::tcp::socket socket,
                     bool use_tls) -> caf::behavior {
  return {};
}

struct connection_manager_state {
  caf::event_based_actor* self;
  bridge_actor bridge;
  operator_ptr read;

  boost::asio::io_service io_service;
  std::optional<boost::asio::ip::tcp::acceptor> acceptor;

  std::vector<caf::actor> connections;

  auto run() -> void {
    detail::weak_run_delayed(self, duration::zero(), [this] {
      using boost::asio::ip::tcp;
      auto socket = tcp::socket(io_service);
      acceptor->accept(socket);
      connections.push_back(
        self->spawn(make_connection, std::move(socket), false));
      run();
    });
  }
};

auto make_connection_manager(
  caf::stateful_actor<connection_manager_state>* self, bridge_actor bridge,
  operator_ptr read, boost::asio::ip::tcp::endpoint endpoint) -> caf::behavior {
  using boost::asio::ip::tcp;
  self->state.self = self;
  self->state.bridge = std::move(bridge);
  self->state.read = std::move(read);
  self->state.acceptor
    = tcp::acceptor(self->state.io_service, std::move(endpoint));
  self->state.run();
  return {};
}

struct bridge_state {
  std::queue<table_slice> buffer;
  caf::typed_response_promise<table_slice> buffer_rp;

  caf::actor connection_manager = {};
};

auto make_bridge(bridge_actor::stateful_pointer<bridge_state> self)
  -> bridge_actor::behavior_type {
  self->state.connection_manager
    = self->spawn(make_connection_manager, bridge_actor{self});
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

class accept_operator final : public crtp_operator<accept_operator> {
public:
  template <operator_input_batch T>
  auto operator()(T x) const -> T {
    return x;
  }

  auto name() const -> std::string override {
    return "accept";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, nullptr};
  }

  friend auto inspect(auto& f, accept_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<accept_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    argument_parser{"accept", "https://docs.tenzir.com/operators/accept"}.parse(
      p);
    return std::make_unique<accept_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::accept

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept::plugin)
