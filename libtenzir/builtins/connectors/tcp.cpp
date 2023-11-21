//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/config.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <boost/asio.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <regex>
#include <system_error>

using namespace std::chrono_literals;

namespace tenzir::plugins::tcp {

// The grand TODO list:
// - TLS
// - bind/connect option
// - deadline handling

namespace {

using tcp_bridge_actor = caf::typed_actor<
  // Connect to a TCP endpoint.
  auto(atom::connect, std::string hostname, std::string port)->caf::result<void>,
  // Read up to the number of events from the TCP endpoint.
  auto(atom::read, uint64_t buffer_size)->caf::result<chunk_ptr>>;

struct tcp_bridge_state {
  static constexpr auto name = "tcp-loader-bridge";

  std::shared_ptr<boost::asio::io_context> io_ctx = {};
  std::optional<boost::asio::ip::tcp::socket> socket = {};
  caf::typed_response_promise<void> connect_rp = {};
  caf::typed_response_promise<chunk_ptr> read_rp = {};
  std::vector<char> read_buffer = {};
};

auto make_tcp_bridge(tcp_bridge_actor::stateful_pointer<tcp_bridge_state> self)
  -> tcp_bridge_actor::behavior_type {
  self->state.io_ctx = std::make_shared<boost::asio::io_context>();
  self->state.socket.emplace(*self->state.io_ctx);
  auto worker = std::thread([io_ctx = self->state.io_ctx]() {
    auto guard = boost::asio::make_work_guard(*io_ctx);
    io_ctx->run();
  });
  self->attach_functor(
    [worker = std::move(worker), io_ctx = self->state.io_ctx]() mutable {
      io_ctx->stop();
      worker.join();
    });
  return {
    [self](atom::connect, const std::string& hostname,
           const std::string& port) -> caf::result<void> {
      if (self->state.connect_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot connect while a connect "
                                           "request is pending",
                                           *self));
      }
      auto resolver = boost::asio::ip::tcp::resolver{*self->state.io_ctx};
      auto endpoints = resolver.resolve(hostname, port);
      // TODO: use the error code overload for resolve.
      self->state.connect_rp = self->make_response_promise<void>();
      auto on_connect = [self, weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(
                                 self)](boost::system::error_code ec,
                                        const boost::asio::ip::tcp::endpoint&) {
        if (auto hdl = weak_hdl.lock()) {
          caf::anon_send(caf::actor_cast<caf::actor>(hdl),
                         caf::make_action([self] {
                           // TODO: if ec is an error, we need to deliver an
                           // error here.
                           self->state.connect_rp.deliver();
                         }));
        }
      };
      boost::asio::async_connect(*self->state.socket, endpoints,
                                 std::move(on_connect));
      return self->state.connect_rp;
    },
    [self](atom::read, uint64_t buffer_size) -> caf::result<chunk_ptr> {
      if (self->state.connect_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot read while a connect "
                                           "request is pending",
                                           *self));
      }
      if (self->state.read_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot read while a read "
                                           "request is pending",
                                           *self));
      }
      self->state.read_buffer.resize(buffer_size);
      self->state.read_rp = self->make_response_promise<chunk_ptr>();
      auto on_read = [self, weak_hdl
                            = caf::actor_cast<caf::weak_actor_ptr>(self)](
                       boost::system::error_code ec, size_t length) {
        if (auto hdl = weak_hdl.lock()) {
          // TODO: Potential optimization: We could at this point already
          // eagerly start the next read
          caf::anon_send(caf::actor_cast<caf::actor>(hdl),
                         caf::make_action([self, ec, length] {
                           if (ec) {
                             self->state.read_rp.deliver(caf::make_error(
                               ec::system_error,
                               fmt::format("failed to read from TCP socket: {}",
                                           ec.message())));
                             return;
                           }
                           self->state.read_buffer.resize(length);
                           self->state.read_buffer.shrink_to_fit();
                           self->state.read_rp.deliver(chunk::make(
                             std::exchange(self->state.read_buffer, {})));
                         }));
        }
      };
      self->state.socket->async_read_some(
        boost::asio::buffer(self->state.read_buffer.data(), buffer_size),
        std::move(on_read));
      return self->state.read_rp;
    },
  };
}

struct connector_args {
  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.tcp.connector_args")
      .fields(f.field("hostname", x.hostname), f.field("port", x.port));
  }

  std::string hostname = {};
  std::string port = {};
};

class loader final : public plugin_loader {
public:
  loader() = default;

  explicit loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make
      = [](connector_args args,
           operator_control_plane& ctrl) mutable -> generator<chunk_ptr> {
      auto tcp_bridge = ctrl.self().spawn(make_tcp_bridge);
      ctrl.self()
        .request(tcp_bridge, caf::infinite, atom::connect_v, args.hostname,
                 args.port)
        .await(
          [&]() {
            // nop
          },
          [&](const caf::error& err) {
            diagnostic::error("failed to connect: {}", err)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      auto result = chunk_ptr{};
      auto running = true;
      while (running) {
        ctrl.self()
          .request(tcp_bridge, caf::infinite, atom::read_v, uint64_t{65536})
          .await(
            [&](chunk_ptr& chunk) {
              result = std::move(chunk);
            },
            [&](const caf::error& err) {
              // TODO Debug?
              TENZIR_INFO("TCP loader disconnected: {}", err);
              running = false;
            });
        co_yield std::exchange(result, {});
      }
    };
    return make(args_, ctrl);
  }

  auto name() const -> std::string override {
    return "tcp";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, loader& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.tcp.loader")
      .fields(f.field("args", x.args_));
  }

  // Remove once the base class has this function removed.
  auto to_string() const -> std::string override {
    return "tcp";
  }

private:
  connector_args args_;
};

class plugin final : public virtual loader_plugin<loader> {
  /// Auto-completes a scheme-less uri with the schem from this plugin.
  static auto remove_scheme(std::string& uri) {
    if (uri.starts_with("tcp://")) {
      uri = std::move(uri).substr(6);
    }
  }

public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto uri = std::string{};
    parser.add(uri, "<uri>");
    parser.parse(p);
    remove_scheme(uri);
    auto split = detail::split(uri, ":", 1);
    TENZIR_ASSERT(split.size() == 2); // FIXME diag
    auto args = connector_args{
      .hostname = std::string{split[0]},
      .port = std::string{split[1]},
    };
    return std::make_unique<loader>(std::move(args));
  }

  auto name() const -> std::string override {
    return "tcp";
  }
};

} // namespace

} // namespace tenzir::plugins::tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tcp::plugin)
