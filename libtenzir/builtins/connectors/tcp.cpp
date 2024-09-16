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
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <filesystem>
#include <regex>
#include <system_error>

using namespace std::chrono_literals;

namespace tenzir::plugins::tcp {

namespace {

using tcp_bridge_actor = caf::typed_actor<
  // Connect to a TCP endpoint.
  auto(atom::connect, bool tls, std::string hostname, std::string port)
    ->caf::result<void>,
  // Wait for an incoming TCP connection.
  auto(atom::accept, std::string hostname, std::string port,
       std::string tls_certfile, std::string tls_keyfile)
    ->caf::result<void>,
  // Read up to the number of bytes from the socket.
  auto(atom::read, uint64_t buffer_size)->caf::result<chunk_ptr>,
  // Write a chunk to the socket.
  auto(atom::write, chunk_ptr chunk)->caf::result<void>>;

struct tcp_metrics {
  auto emit() -> void {
    if (reads == 0 and writes == 0 and handle.empty()) {
      return;
    }
    metric_handler.emit({
      {"port", port},
      {"handle", handle},
      {"reads", reads},
      {"writes", writes},
      {"bytes_read", bytes_read},
      {"bytes_written", bytes_written},
    });
    reads = 0;
    writes = 0;
    bytes_read = 0;
    bytes_written = 0;
  }
  class metric_handler metric_handler = {};

  uint16_t port = {};
  std::string handle = {};

  uint64_t reads = {};
  uint64_t writes = {};
  uint64_t bytes_read = {};
  uint64_t bytes_written = {};
};

struct tcp_bridge_state {
  static constexpr auto name = "tcp-loader-bridge";

  tcp_bridge_state() = default;

  ~tcp_bridge_state() noexcept {
    io_ctx->stop();
    worker.join();
    metrics.emit();
  }

  // The `io_context` running the async callbacks.
  std::shared_ptr<boost::asio::io_context> io_ctx = {};
  std::thread worker = {};

  // The TCP socket holding our connection.
  // (always exists, but wrapped in an optional because `socket`
  //  isn't default-constructible)
  std::optional<boost::asio::ip::tcp::socket> socket = {};

  // TLS stream wrapping `socket` if we're in TLS mode.
  std::optional<boost::asio::ssl::context> ssl_ctx = {};
  std::optional<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>>
    tls_socket = {};

  // Acceptor if we're in 'listen' mode.
  std::optional<boost::asio::ip::tcp::acceptor> acceptor = {};

  // Promise that is delivered when a connection is established.
  caf::typed_response_promise<void> connection_rp = {};

  // Promise that is delivered whenever new data arrives.
  caf::typed_response_promise<chunk_ptr> read_rp = {};

  // Promise that is delivered whenever new data is sent.
  caf::typed_response_promise<void> write_rp = {};

  // Storage for incoming data.
  std::vector<char> read_buffer = {};

  // Metrics.
  tcp_metrics metrics = {};
};

auto make_tcp_bridge(tcp_bridge_actor::stateful_pointer<tcp_bridge_state> self,
                     metric_handler metric_handler)
  -> tcp_bridge_actor::behavior_type {
  self->state.io_ctx = std::make_shared<boost::asio::io_context>();
  self->state.socket.emplace(*self->state.io_ctx);
  self->state.worker = std::thread([io_ctx = self->state.io_ctx]() {
    auto guard = boost::asio::make_work_guard(*io_ctx);
    io_ctx->run();
  });
  self->state.metrics.metric_handler = std::move(metric_handler);
  detail::weak_run_delayed_loop(
    self, std::chrono::seconds{1},
    [self] {
      self->state.metrics.emit();
    },
    /*run_immediately=*/false);
  return {
    [self](atom::connect, bool tls, const std::string& hostname,
           const std::string& service) -> caf::result<void> {
      if (self->state.connection_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot connect while a connect "
                                           "request is pending",
                                           *self));
      }
      auto resolver = boost::asio::ip::tcp::resolver{*self->state.io_ctx};
      auto ec = boost::system::error_code{};
      auto endpoints = resolver.resolve(hostname, service, ec);
      if (ec) {
        return caf::make_error(ec::system_error,
                               fmt::format("failed to resolve '{}': {}",
                                           hostname, ec.message()));
      }
#if TENZIR_LINUX
      const auto& endpoint = endpoints.begin()->endpoint();
      auto sfd
        = ::socket(endpoint.protocol().family(), SOCK_STREAM | SOCK_CLOEXEC,
                   endpoint.protocol().protocol());
      TENZIR_ASSERT(sfd >= 0);
      self->state.socket->assign(endpoint.protocol(), sfd);
#endif
      if (tls) {
        self->state.ssl_ctx.emplace(boost::asio::ssl::context::tls_client);
        self->state.ssl_ctx->set_default_verify_paths();
        self->state.ssl_ctx->set_verify_mode(
          boost::asio::ssl::verify_peer
          | boost::asio::ssl::verify_fail_if_no_peer_cert);
        self->state.tls_socket.emplace(*self->state.socket,
                                       *self->state.ssl_ctx);
        auto tls_handle = self->state.tls_socket->native_handle();
        if (SSL_set1_host(tls_handle, hostname.c_str()) != 1) {
          return caf::make_error(ec::system_error,
                                 "failed to enable host name verification");
        }
        if (not SSL_set_tlsext_host_name(tls_handle, hostname.c_str())) {
          return caf::make_error(ec::system_error, "failed to set SNI");
        }
      }
      self->state.connection_rp = self->make_response_promise<void>();
      boost::asio::async_connect(
        *self->state.socket, endpoints,
        [self, weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(self)](
          boost::system::error_code ec,
          const boost::asio::ip::tcp::endpoint& endpoint) {
#if TENZIR_MACOS
          auto fcntl_error = std::optional<caf::error>{};
          if (::fcntl(self->state.socket->native_handle(), F_SETFD, FD_CLOEXEC)
              != 0) {
            auto error = detail::describe_errno();
            fcntl_error = diagnostic::error("failed to configure TLS socket")
                            .hint("{}", error)
                            .to_error();
          }
#endif
          self->state.metrics.port = endpoint.port();
          self->state.metrics.handle
            = fmt::to_string(self->state.socket->native_handle());
          if (auto hdl = weak_hdl.lock()) {
            caf::anon_send(
              caf::actor_cast<caf::actor>(hdl),
              caf::make_action([self, ec, endpoint
#if TENZIR_MACOS
                                ,
                                fcntl_error
#endif
            ]() mutable {
                if (ec) {
                  return self->state.connection_rp.deliver(caf::make_error(
                    ec::system_error,
                    fmt::format("connection failed: {}", ec.message())));
                }
#if TENZIR_MACOS
                if (fcntl_error) {
                  return self->state.connection_rp.deliver(*fcntl_error);
                }
#endif
                if (self->state.tls_socket) {
                  self->state.tls_socket->handshake(
                    boost::asio::ssl::stream<
                      boost::asio::ip::tcp::socket>::client,
                    ec);
                  if (ec) {
                    return self->state.connection_rp.deliver(caf::make_error(
                      ec::system_error,
                      fmt::format("TLS client handshake failed: {}",
                                  ERR_get_error())));
                  }
                }
                TENZIR_VERBOSE("tcp connector connected to {}",
                               endpoint.address().to_string());
                return self->state.connection_rp.deliver();
              }));
          }
        });
      return self->state.connection_rp;
    },
    [self](atom::accept, const std::string& hostname,
           const std::string& service, const std::string& certfile,
           const std::string& keyfile) -> caf::result<void> {
      auto ec = boost::system::error_code{};
      auto resolver = boost::asio::ip::tcp::resolver{*self->state.io_ctx};
      auto endpoints = resolver.resolve(hostname, service, ec);
      if (ec || endpoints.empty()) {
        return caf::make_error(
          ec::system_error, fmt::format("failed to resolve host {}, service {}",
                                        hostname, service));
      }
      auto resolver_entry = *endpoints.begin();
      auto endpoint = resolver_entry.endpoint();
      self->state.metrics.port = endpoint.port();
      // Create a new acceptor and bind to provided endpoint.
      try {
        self->state.acceptor.emplace(*self->state.io_ctx);
        self->state.acceptor->open(endpoint.protocol());
        auto reuse_address = boost::asio::socket_base::reuse_address(true);
        self->state.acceptor->set_option(reuse_address);
        self->state.acceptor->bind(endpoint);
        if (::fcntl(self->state.acceptor->native_handle(), F_SETFD, FD_CLOEXEC)
            != 0) {
          auto error = detail::describe_errno();
          return diagnostic::error("failed to configure TLS socket")
            .hint("{}", error)
            .to_error();
        }
        auto backlog = boost::asio::socket_base::max_connections;
        self->state.acceptor->listen(backlog);
        self->state.metrics.handle
          = fmt::to_string(self->state.acceptor->native_handle());
      } catch (std::exception& e) {
        return caf::make_error(ec::system_error,
                               fmt::format("failed to bind to endpoint: {}",
                                           e.what()));
      }
      TENZIR_VERBOSE("tcp connector listens on endpoint {}:{}",
                     endpoint.address().to_string(), endpoint.port());
      self->state.connection_rp = self->make_response_promise<void>();
      self->state.acceptor->async_accept(
        [self, certfile, keyfile,
         weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(self)](
          boost::system::error_code ec, boost::asio::ip::tcp::socket peer) {
          TENZIR_VERBOSE("tcp connector accepted incoming request");
          auto fcntl_error = std::optional<caf::error>{};
          if (::fcntl(peer.native_handle(), F_SETFD, FD_CLOEXEC) != 0) {
            auto error = detail::describe_errno();
            fcntl_error = diagnostic::error("failed to configure TLS socket")
                            .hint("{}", error)
                            .to_error();
          }
          if (auto hdl = weak_hdl.lock()) {
            caf::anon_send(
              caf::actor_cast<caf::actor>(hdl),
              caf::make_action([self, certfile, keyfile, ec,
                                peer = std::move(peer),
                                fcntl_error
                                = std::move(fcntl_error)]() mutable {
                if (ec) {
                  return self->state.connection_rp.deliver(caf::make_error(
                    ec::system_error,
                    fmt::format("failed to accept: {}", ec.message())));
                }
                if (fcntl_error) {
                  return self->state.connection_rp.deliver(*fcntl_error);
                }
                self->state.socket.emplace(std::move(peer));
                if (!certfile.empty()) {
                  self->state.ssl_ctx.emplace(
                    boost::asio::ssl::context::tls_server);
                  self->state.ssl_ctx->use_certificate_chain_file(certfile);
                  self->state.ssl_ctx->use_private_key_file(
                    keyfile, boost::asio::ssl::context::pem);
                  self->state.ssl_ctx->set_verify_mode(
                    boost::asio::ssl::verify_none);
                  self->state.tls_socket.emplace(*self->state.socket,
                                                 *self->state.ssl_ctx);
                  auto server_context = boost::asio::ssl::stream<
                    boost::asio::ip::tcp::socket>::server;
                  self->state.tls_socket->handshake(server_context, ec);
                  if (ec) {
                    return self->state.connection_rp.deliver(caf::make_error(
                      ec::system_error,
                      fmt::format("TLS handshake failed: {}", ec.message())));
                  }
                }
                return self->state.connection_rp.deliver();
              }));
          }
        });
      return self->state.connection_rp;
    },
    [self](atom::read, uint64_t buffer_size) -> caf::result<chunk_ptr> {
      if (self->state.connection_rp.pending()) {
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
          // eagerly start the next read.
          caf::anon_send(caf::actor_cast<caf::actor>(hdl),
                         caf::make_action([self, ec, length] {
                           if (ec) {
                             return self->state.read_rp.deliver(caf::make_error(
                               ec::system_error,
                               fmt::format("failed to read from TCP socket: {}",
                                           ec.message())));
                           }
                           self->state.metrics.reads++;
                           self->state.metrics.bytes_read += length;
                           self->state.read_buffer.resize(length);
                           self->state.read_buffer.shrink_to_fit();
                           return self->state.read_rp.deliver(chunk::make(
                             std::exchange(self->state.read_buffer, {})));
                         }));
        }
      };
      auto asio_buffer
        = boost::asio::buffer(self->state.read_buffer, buffer_size);
      if (self->state.tls_socket) {
        self->state.tls_socket->async_read_some(asio_buffer, on_read);
      } else {
        self->state.socket->async_read_some(asio_buffer, on_read);
      }
      return self->state.read_rp;
    },
    [self](atom::write, chunk_ptr chunk) -> caf::result<void> {
      if (self->state.connection_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot write while a connect "
                                           "request is pending",
                                           *self));
      }
      if (self->state.write_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot write while a write "
                                           "request is pending",
                                           *self));
      }
      self->state.write_rp = self->make_response_promise<void>();
      auto on_write = [self, chunk,
                       weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(self)](
                        boost::system::error_code ec, size_t length) {
        if (auto hdl = weak_hdl.lock()) {
          caf::anon_send(caf::actor_cast<caf::actor>(hdl),
                         caf::make_action([self, chunk, ec, length] {
                           if (ec) {
                             self->state.write_rp.deliver(caf::make_error(
                               ec::system_error,
                               fmt::format("failed to write to TCP socket: {}",
                                           ec.message())));
                             return;
                           }
                           self->state.metrics.writes++;
                           self->state.metrics.bytes_written += length;
                           if (length < chunk->size()) {
                             auto remainder = chunk->slice(length);
                             self->state.write_rp.delegate(
                               static_cast<tcp_bridge_actor>(self),
                               atom::write_v, std::move(remainder));
                             return;
                           }
                           TENZIR_ASSERT(length == chunk->size());
                           self->state.write_rp.deliver();
                         }));
        }
      };
      auto asio_buffer = boost::asio::buffer(chunk->data(), chunk->size());
      if (self->state.tls_socket) {
        self->state.tls_socket->async_write_some(asio_buffer, on_write);
      } else {
        self->state.socket->async_write_some(asio_buffer, on_write);
      }
      return self->state.write_rp;
    },
  };
}

struct connector_args {
  std::string hostname = {};
  std::string port = {};
  bool listen_once = false;
  bool tls = false;
  std::optional<std::string> tls_certfile = {};
  std::optional<std::string> tls_keyfile = {};
};

struct loader_args : connector_args {
  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.tcp.loader_args")
      .fields(f.field("hostname", x.hostname), f.field("port", x.port),
              f.field("listen_once", x.listen_once),
              f.field("connect", x.connect), f.field("tls", x.tls),
              f.field("tls_certfile", x.tls_certfile),
              f.field("tls_keyfile", x.tls_keyfile));
  }

  bool connect = false;
};

struct saver_args : connector_args {
  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.tcp.saver_args")
      .fields(f.field("hostname", x.hostname), f.field("port", x.port),
              f.field("listen_once", x.listen_once),
              f.field("listen", x.listen), f.field("tls", x.tls),
              f.field("tls_certfile", x.tls_certfile),
              f.field("tls_keyfile", x.tls_keyfile));
  }

  bool listen = false;
};

class loader final : public plugin_loader {
public:
  loader() = default;

  explicit loader(loader_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    if (args_.tls and not args_.connect) {
      // Verify that the files actually exist and are readable.
      // Ideally we'd also like to verify that the files contain valid
      // key material, but there's no straightforward API for this in OpenSSL.
      TENZIR_ASSERT(args_.tls_keyfile);
      TENZIR_ASSERT(args_.tls_certfile);
      auto* keyfile = std::fopen(args_.tls_keyfile->c_str(), "r");
      if (keyfile) {
        std::fclose(keyfile);
      } else {
        auto error = detail::describe_errno();
        diagnostic::error("failed to open TLS keyfile")
          .hint("{}", error)
          .emit(ctrl.diagnostics());
        return {};
      }
      auto* certfile = std::fopen(args_.tls_certfile->c_str(), "r");
      if (certfile) {
        std::fclose(certfile);
      } else {
        auto error = detail::describe_errno();
        diagnostic::error("failed to open TLS certfile")
          .hint("{}", error)
          .emit(ctrl.diagnostics());
        return {};
      }
    }
    auto make
      = [](loader_args args,
           operator_control_plane& ctrl) mutable -> generator<chunk_ptr> {
      auto tcp_bridge = ctrl.self().spawn(make_tcp_bridge,
                                          ctrl.metrics({
                                            "tenzir.metrics.tcp",
                                            record_type{
                                              {"port", uint64_type{}},
                                              {"handle", string_type{}},
                                              {"reads", uint64_type{}},
                                              {"writes", uint64_type{}},
                                              {"bytes_read", uint64_type{}},
                                              {"bytes_written", uint64_type{}},
                                            },
                                          }));
      do {
        if (args.connect) {
          ctrl.self()
            .request(tcp_bridge, caf::infinite, atom::connect_v, args.tls,
                     args.hostname, args.port)
            .await(
              [&]() {
                // nop
              },
              [&](const caf::error& err) {
                diagnostic::error("failed to connect: {}", err)
                  .emit(ctrl.diagnostics());
              });
        } else {
          ctrl.self()
            .request(tcp_bridge, caf::infinite, atom::accept_v, args.hostname,
                     args.port, args.tls_certfile.value_or(std::string{}),
                     args.tls_keyfile.value_or(std::string{}))
            .await(
              [&]() {
                // nop
              },
              [&](const caf::error& err) {
                diagnostic::error("failed to listen: {}", err)
                  .emit(ctrl.diagnostics());
              });
        }
        co_yield {};
        // Read and forward incoming data.
        auto result = chunk_ptr{};
        auto running = true;
        while (running) {
          constexpr auto buffer_size = uint64_t{65'536};
          ctrl.self()
            .request(tcp_bridge, caf::infinite, atom::read_v, buffer_size)
            .await(
              [&](chunk_ptr& chunk) {
                result = std::move(chunk);
              },
              [&](const caf::error& err) {
                TENZIR_DEBUG("tcp connector encountered error: {}", err);
                running = false;
              });
          co_yield std::exchange(result, {});
        }
      } while (not args.connect and not args.listen_once);
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

private:
  loader_args args_;
};

class saver final : public plugin_saver {
public:
  saver() = default;

  explicit saver(saver_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    if (args_.tls and args_.listen) {
      // Verify that the files actually exist and are readable.
      // Ideally we'd also like to verify that the files contain valid
      // key material, but there's no straightforward API for this in OpenSSL.
      TENZIR_ASSERT(args_.tls_keyfile);
      TENZIR_ASSERT(args_.tls_certfile);
      auto* keyfile = std::fopen(args_.tls_keyfile->c_str(), "r");
      if (keyfile) {
        std::fclose(keyfile);
      } else {
        auto error = detail::describe_errno();
        diagnostic::error("failed to open TLS keyfile")
          .hint("{}", error)
          .emit(ctrl.diagnostics());
        return caf::make_error(ec::invalid_argument);
      }
      auto* certfile = std::fopen(args_.tls_certfile->c_str(), "r");
      if (certfile) {
        std::fclose(certfile);
      } else {
        auto error = detail::describe_errno();
        diagnostic::error("failed to open TLS certfile")
          .hint("{}", error)
          .emit(ctrl.diagnostics());
        return caf::make_error(ec::invalid_argument);
      }
    }
    auto tcp_bridge
      = ctrl.self().spawn(make_tcp_bridge, ctrl.metrics({
                                             "tenzir.metrics.tcp",
                                             record_type{
                                               {"port", uint64_type{}},
                                               {"handle", string_type{}},
                                               {"reads", uint64_type{}},
                                               {"writes", uint64_type{}},
                                               {"bytes_read", uint64_type{}},
                                               {"bytes_written", uint64_type{}},
                                             },
                                           }));

    if (not args_.listen) {
      ctrl.self()
        .request(tcp_bridge, caf::infinite, atom::connect_v, args_.tls,
                 args_.hostname, args_.port)
        .await(
          [&]() {
            // nop
          },
          [&](const caf::error& err) {
            diagnostic::error("failed to connect: {}", err)
              .emit(ctrl.diagnostics());
          });
    } else {
      ctrl.self()
        .request(tcp_bridge, caf::infinite, atom::accept_v, args_.hostname,
                 args_.port, args_.tls_certfile.value_or(std::string{}),
                 args_.tls_keyfile.value_or(std::string{}))
        .await(
          [&]() {
            // nop
          },
          [&](const caf::error& err) {
            diagnostic::error("failed to listen: {}", err)
              .emit(ctrl.diagnostics());
          });
    }
    return [&ctrl, tcp_bridge](chunk_ptr chunk) mutable {
      if (not chunk || chunk->size() == 0)
        return;
      ctrl.self()
        .request(tcp_bridge, caf::infinite, atom::write_v, std::move(chunk))
        .await(
          [&]() {
            // nop
          },
          [&](const caf::error& err) {
            diagnostic::error("tcp connector encountered error: {}", err)
              .emit(ctrl.diagnostics());
          });
    };
  }

  auto name() const -> std::string override {
    return "tcp";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, saver& x) -> bool {
    return f.object(x).pretty_name("saver").fields(f.field("args", x.args_));
  }

private:
  saver_args args_;
};

class plugin final : public virtual loader_plugin<loader>, saver_plugin<saver> {
  /// Auto-completes a scheme-less URI with the scheme from this plugin.
  static auto remove_scheme(std::string& uri) {
    if (uri.starts_with("tcp://")) {
      uri = std::move(uri).substr(6);
    }
  }

public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = parse_args<loader_args>(p);
    return std::make_unique<loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = parse_args<saver_args>(p);
    return std::make_unique<saver>(std::move(args));
  }

  template <class Args>
  auto parse_args(parser_interface& p) const -> Args {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = Args{};
    auto uri = located<std::string>{};
    parser.add(uri, "<endpoint>");
    if constexpr (std::is_same_v<Args, loader_args>) {
      parser.add("-c,--connect", args.connect);
    } else if constexpr (std::is_same_v<Args, saver_args>) {
      parser.add("-l,--listen", args.listen);
    }
    parser.add("-o,--listen-once", args.listen_once);
    parser.add("--tls", args.tls);
    parser.add("--certfile", args.tls_certfile, "TLS certificate");
    parser.add("--keyfile", args.tls_keyfile, "TLS private key");
    parser.parse(p);
    remove_scheme(uri.inner);
    auto split = detail::split(uri.inner, ":", 1);
    if (split.size() != 2) {
      diagnostic::error("malformed endpoint")
        .primary(uri.source)
        .hint("format must be 'tcp://address:port'")
        .throw_();
    } else {
      args.hostname = std::string{split[0]};
      args.port = std::string{split[1]};
    }
    if (not args.tls) {
      if (args.tls_certfile and not args.tls_certfile->empty()) {
        diagnostic::error("certificate provided, but TLS disabled")
          .hint("add --tls to use an encrypted connection")
          .throw_();
      }
      if (args.tls_keyfile and not args.tls_keyfile->empty()) {
        diagnostic::error("keyfile provided, but TLS disabled")
          .hint("add --tls to use an encrypted connection")
          .throw_();
      }
    }
    if constexpr (std::is_same_v<Args, loader_args>) {
      if (args.listen_once and args.connect) {
        diagnostic::error("conflicting options `--connect` and `--listen-once`")
          .throw_();
      }
      if (not args.connect and args.tls) {
        if (not args.tls_certfile or args.tls_certfile->empty()) {
          diagnostic::error("invalid TLS settings")
            .hint("missing --certfile")
            .throw_();
        }
        if (not args.tls_keyfile or args.tls_keyfile->empty()) {
          diagnostic::error("invalid TLS settings")
            .hint("missing --keyfile")
            .throw_();
        }
      }
    }
    if constexpr (std::is_same_v<Args, saver_args>) {
      if (args.listen_once) {
        args.listen = true;
      }
    }
    return args;
  }

  auto name() const -> std::string override {
    return "tcp";
  }
};

} // namespace

} // namespace tenzir::plugins::tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tcp::plugin)
