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
#include <tenzir/ssl_options.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/version.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/anon_mail.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>

using namespace std::chrono_literals;

namespace tenzir::plugins::tcp {

namespace {

struct tcp_bridge_actor_traits {
  using signatures = caf::type_list<
    // Connect to a TCP endpoint.
    auto(atom::connect, bool tls, std::string cacert, std::string certfile,
         std::string keyfile, bool skip_peer_verification, std::string hostname,
         std::string port)
      ->caf::result<void>,
    // Wait for an incoming TCP connection.
    auto(atom::accept, std::string hostname, std::string port,
         std::string tls_certfile, std::string tls_keyfile)
      ->caf::result<void>,
    // Write a chunk to the socket.
    auto(atom::write, chunk_ptr chunk)->caf::result<void>>;
};

using tcp_bridge_actor = caf::typed_actor<tcp_bridge_actor_traits>;

struct tcp_metrics {
  auto emit() -> void {
    if (reads == 0 and writes == 0 and handle.empty()) {
      return;
    }
    metric_handler.emit({
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
  class metric_handler metric_handler;

  uint16_t port = {};
  std::string handle;

  uint64_t reads = {};
  uint64_t writes = {};
  uint64_t bytes_read = {};
  uint64_t bytes_written = {};
};

class tcp_bridge {
public:
  [[maybe_unused]] static constexpr auto name = "tcp-bridge";

  tcp_bridge(tcp_bridge_actor::pointer self, metric_handler metric_handler)
    : self_{self},
      max_retries_{5},
      initial_retry_delay_ms_{1000},
      retry_backoff_multiplier_{2.0},
      max_retry_delay_ms_{30000} {
    io_ctx_ = std::make_shared<boost::asio::io_context>();
    socket_.emplace(*io_ctx_);
    worker_ = std::thread([io_ctx = io_ctx_]() {
      auto guard = boost::asio::make_work_guard(*io_ctx);
      io_ctx->run();
    });
    metrics_.metric_handler = std::move(metric_handler);
    detail::weak_run_delayed_loop(
      self_, std::chrono::seconds{1},
      [this] {
        metrics_.emit();
      },
      /*run_immediately=*/false);
  }

  ~tcp_bridge() noexcept {
    io_ctx_->stop();
    worker_.join();
    metrics_.emit();
  }

  auto make_behavior() -> tcp_bridge_actor::behavior_type {
    return {
      [this](atom::connect, bool tls, const std::string& cacert,
             const std::string& certfile, const std::string& keyfile,
             bool skip_peer_verification, const std::string& hostname,
             const std::string& service) -> caf::result<void> {
        return connect(tls, cacert, certfile, keyfile, skip_peer_verification,
                       hostname, service);
      },
      [this](atom::accept, const std::string& hostname,
             const std::string& service, const std::string& tls_certfile,
             const std::string& tls_keyfile) -> caf::result<void> {
        return accept(hostname, service, tls_certfile, tls_keyfile);
      },
      [this](atom::write, chunk_ptr chunk) -> caf::result<void> {
        return write(std::move(chunk));
      },
    };
  }

private:
  auto connect(bool tls, const std::string& cacert, const std::string& certfile,
               const std::string& keyfile, bool skip_peer_verification,
               const std::string& hostname, const std::string& service)
    -> caf::result<void> {
    if (connection_rp_.pending()) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} cannot connect while a connect "
                                         "request is pending",
                                         *self_));
    }

    // Store connection parameters for retry attempts
    connect_params_ = {.tls = tls,
                       .cacert = cacert,
                       .certfile = certfile,
                       .keyfile = keyfile,
                       .skip_peer_verification = skip_peer_verification,
                       .hostname = hostname,
                       .service = service};
    current_retry_attempt_ = 0;
    is_connected_ = false;
    connection_rp_ = self_->make_response_promise<void>();

    // Start the first connection attempt
    attempt_connect();
    return connection_rp_;
  }

  void attempt_connect() {
    auto& params = *connect_params_;

    auto resolver = boost::asio::ip::tcp::resolver{*io_ctx_};
    auto ec = boost::system::error_code{};
    auto endpoints = resolver.resolve(params.hostname, params.service, ec);
    if (ec) {
      handle_connection_failure(caf::make_error(
        ec::system_error, fmt::format("failed to resolve '{}': {}",
                                      params.hostname, ec.message())));
      return;
    }

    // Reset socket for retry attempts
    if (current_retry_attempt_ > 0) {
      socket_.emplace(*io_ctx_);
    }

#if TENZIR_LINUX
    const auto& endpoint = endpoints.begin()->endpoint();
    auto sfd
      = ::socket(endpoint.protocol().family(), SOCK_STREAM | SOCK_CLOEXEC,
                 endpoint.protocol().protocol());
    TENZIR_ASSERT(sfd >= 0);
    socket_->assign(endpoint.protocol(), sfd);
#endif
    if (params.tls) {
      ssl_ctx_.emplace(boost::asio::ssl::context::tls_client);
      ssl_ctx_->set_default_verify_paths();
      auto ec = boost::system::error_code{};
      if (params.skip_peer_verification) {
        ssl_ctx_->set_verify_mode(boost::asio::ssl::verify_none);
      } else {
        ssl_ctx_->set_verify_mode(
          boost::asio::ssl::verify_peer
          | boost::asio::ssl::verify_fail_if_no_peer_cert);
        if (not params.cacert.empty()) {
          if (ssl_ctx_->load_verify_file(params.cacert, ec).failed()) {
            handle_connection_failure(caf::make_error(
              ec::system_error, fmt::format("failed to load cacert file "
                                            "`{}`: {}",
                                            params.cacert, ec.message())));
            return;
          }
        }
      }
      if (not params.certfile.empty()) {
        if (ssl_ctx_->use_certificate_chain_file(params.certfile, ec).failed()) {
          handle_connection_failure(caf::make_error(
            ec::system_error, fmt::format("failed to load certfile `{}`: {}",
                                          params.certfile, ec.message())));
          return;
        }
      }
      if (not params.keyfile.empty()) {
        if (ssl_ctx_
              ->use_private_key_file(params.keyfile,
                                     boost::asio::ssl::context::pem, ec)
              .failed()) {
          handle_connection_failure(caf::make_error(
            ec::system_error, fmt::format("failed to load keyfile `{}`: {}",
                                          params.keyfile, ec.message())));
          return;
        }
      }
      tls_socket_.emplace(*socket_, *ssl_ctx_);
      auto* tls_handle = tls_socket_->native_handle();
      if (SSL_set1_host(tls_handle, params.hostname.c_str()) != 1) {
        handle_connection_failure(caf::make_error(
          ec::system_error, "failed to enable host name verification"));
        return;
      }
      if (not SSL_set_tlsext_host_name(tls_handle, params.hostname.c_str())) {
        handle_connection_failure(
          caf::make_error(ec::system_error, "failed to set SNI"));
        return;
      }
    }

    boost::asio::async_connect(
      *socket_, endpoints,
      [this, weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(self_)](
        boost::system::error_code ec,
        const boost::asio::ip::tcp::endpoint& endpoint) {
#if TENZIR_MACOS
        auto fcntl_error = std::optional<caf::error>{};
        if (ec == boost::system::errc::success
            && ::fcntl(socket_->native_handle(), F_SETFD, FD_CLOEXEC) != 0) {
          auto error = detail::describe_errno();
          fcntl_error = diagnostic::error("failed to configure TLS socket")
                          .hint("{}", error)
                          .to_error();
        }
#endif
        if (ec == boost::system::errc::success) {
          metrics_.port = endpoint.port();
          metrics_.handle = fmt::to_string(socket_->native_handle());
        }
        if (auto hdl = weak_hdl.lock()) {
          caf::anon_mail(caf::make_action([this, ec, endpoint
#if TENZIR_MACOS
                                           ,
                                           fcntl_error
#endif
          ]() mutable {
            if (ec) {
              handle_connection_failure(caf::make_error(
                ec::system_error,
                fmt::format("connection failed: {}", ec.message())));
              return;
            }
#if TENZIR_MACOS
            if (fcntl_error) {
              handle_connection_failure(*fcntl_error);
              return;
            }
#endif
            if (tls_socket_) {
              tls_socket_->handshake(
                boost::asio::ssl::stream<boost::asio::ip::tcp::socket>::client,
                ec);
              if (ec) {
                handle_connection_failure(
                  caf::make_error(ec::system_error,
                                  fmt::format("TLS client handshake failed: {}",
                                              ERR_get_error())));
                return;
              }
            }
            TENZIR_VERBOSE("tcp connector connected to {} after {} attempt(s)",
                           endpoint.address().to_string(),
                           current_retry_attempt_ + 1);
            is_connected_ = true;
            connection_rp_.deliver();
            process_pending_operations();
          }))
            .send(caf::actor_cast<caf::actor>(hdl));
        }
      });
  }

  void handle_connection_failure(const caf::error& error) {
    current_retry_attempt_++;

    if (current_retry_attempt_ > max_retries_) {
      TENZIR_DEBUG("tcp connector failed to connect after {} attempts: {}",
                   max_retries_, error);

      for (auto& [promise, chunk] : pending_write_promises_) {
        promise.deliver(
          caf::make_error(ec::system_error, "connection failed after retries"));
      }
      pending_write_promises_.clear();

      connection_rp_.deliver(error);
      return;
    }

    // Calculate delay with exponential backoff
    auto delay_ms
      = std::min(static_cast<uint64_t>(initial_retry_delay_ms_
                                       * std::pow(retry_backoff_multiplier_,
                                                  current_retry_attempt_ - 1)),
                 max_retry_delay_ms_);

    TENZIR_INFO("tcp connector connection attempt {} failed, retrying in "
                "{}ms: {}",
                current_retry_attempt_, delay_ms, error);

    // Schedule retry after delay
    retry_timer_.emplace(*io_ctx_);
    retry_timer_->expires_after(std::chrono::milliseconds(delay_ms));
    retry_timer_->async_wait(
      [this, weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(self_)](
        const boost::system::error_code& ec) {
        if (ec) {
          return; // Timer was cancelled
        }
        if (auto hdl = weak_hdl.lock()) {
          caf::anon_mail(caf::make_action([this]() {
            attempt_connect();
          }))
            .send(caf::actor_cast<caf::actor>(hdl));
        }
      });
  }

  // Check if an error indicates a connection was lost
  bool is_disconnection_error(const boost::system::error_code& ec) const {
    return ec == boost::asio::error::connection_reset
           || ec == boost::asio::error::broken_pipe
           || ec == boost::asio::error::connection_aborted
           || ec == boost::asio::error::eof
           || ec == boost::asio::error::network_down
           || ec == boost::asio::error::network_unreachable
           || ec == boost::asio::error::host_unreachable
           || ec == boost::asio::error::connection_refused
           || ec == boost::asio::error::timed_out
           || ec == boost::system::errc::broken_pipe
           || ec == boost::system::errc::connection_reset
           || ec == boost::system::errc::connection_aborted;
  }

  // Handle disconnection by triggering reconnection
  void handle_disconnection() {
    if (! connect_params_ || connection_rp_.pending()) {
      return; // Already reconnecting or no connection params stored
    }

    is_connected_ = false;
    current_retry_attempt_ = 0;

    TENZIR_DEBUG("tcp connector initiating reconnection due to disconnection");

    // Close existing sockets
    if (tls_socket_) {
      boost::system::error_code ec;
      tls_socket_->lowest_layer().close(ec);
      tls_socket_.reset();
    }
    if (socket_) {
      boost::system::error_code ec;
      socket_->close(ec);
    }
    ssl_ctx_.reset();

    // Start reconnection
    connection_rp_ = self_->make_response_promise<void>();
    attempt_connect();
  }

  // Process any pending promises after successful reconnection
  void process_pending_operations() {
    // Restart pending write operations
    for (auto& [promise, chunk] : pending_write_promises_) {
      auto bytes = as_bytes(chunk);
      auto sv = std::string_view(reinterpret_cast<const char*>(bytes.data()),
                                 bytes.size());
      TENZIR_INFO("processing pending write {}", sv);
      promise.delegate(static_cast<tcp_bridge_actor>(self_), atom::write_v,
                       std::move(chunk));
    }
    pending_write_promises_.clear();
  }

  struct connection_params {
    bool tls;
    std::string cacert;
    std::string certfile;
    std::string keyfile;
    bool skip_peer_verification;
    std::string hostname;
    std::string service;
  };

public:
  auto accept(const std::string& hostname, const std::string& service,
              const std::string& certfile, const std::string& keyfile)
    -> caf::result<void> {
    auto ec = boost::system::error_code{};
    auto resolver = boost::asio::ip::tcp::resolver{*io_ctx_};
    auto endpoints = resolver.resolve(hostname, service, ec);
    if (ec || endpoints.empty()) {
      return caf::make_error(
        ec::system_error, fmt::format("failed to resolve host {}, service {}",
                                      hostname, service));
    }
    auto resolver_entry = *endpoints.begin();
    auto endpoint = resolver_entry.endpoint();
    metrics_.port = endpoint.port();
    // Create a new acceptor and bind to provided endpoint.
    try {
      acceptor_.emplace(*io_ctx_);
      acceptor_->open(endpoint.protocol());
      auto reuse_address = boost::asio::socket_base::reuse_address(true);
      acceptor_->set_option(reuse_address);
      acceptor_->bind(endpoint);
      if (::fcntl(acceptor_->native_handle(), F_SETFD, FD_CLOEXEC) != 0) {
        auto error = detail::describe_errno();
        return diagnostic::error("failed to configure TLS socket")
          .hint("{}", error)
          .to_error();
      }
#if BOOST_VERSION >= 108700
      const auto max_connections
        = boost::asio::socket_base::max_listen_connections;
#else
      const auto max_connections = boost::asio::socket_base::max_connections;
#endif
      acceptor_->listen(max_connections);
      metrics_.handle = fmt::to_string(acceptor_->native_handle());
    } catch (std::exception& e) {
      return caf::make_error(ec::system_error,
                             fmt::format("failed to bind to endpoint: {}",
                                         e.what()));
    }
    TENZIR_VERBOSE("tcp connector listens on endpoint {}:{}",
                   endpoint.address().to_string(), endpoint.port());
    connection_rp_ = self_->make_response_promise<void>();
    acceptor_->async_accept([this, certfile, keyfile,
                             weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(
                               self_)](boost::system::error_code ec,
                                       boost::asio::ip::tcp::socket peer) {
      TENZIR_VERBOSE("tcp connector accepted incoming request");
      auto fcntl_error = std::optional<caf::error>{};
      if (::fcntl(peer.native_handle(), F_SETFD, FD_CLOEXEC) != 0) {
        auto error = detail::describe_errno();
        fcntl_error = diagnostic::error("failed to configure TLS socket")
                        .hint("{}", error)
                        .to_error();
      }
      if (auto hdl = weak_hdl.lock()) {
        caf::anon_mail(
          caf::make_action([this, certfile, keyfile, ec, peer = std::move(peer),
                            fcntl_error = std::move(fcntl_error)]() mutable {
            if (ec) {
              return connection_rp_.deliver(caf::make_error(
                ec::system_error,
                fmt::format("failed to accept: {}", ec.message())));
            }
            if (fcntl_error) {
              return connection_rp_.deliver(*fcntl_error);
            }
            socket_.emplace(std::move(peer));
            if (not certfile.empty()) {
              ssl_ctx_.emplace(boost::asio::ssl::context::tls_server);
              ssl_ctx_->use_certificate_chain_file(certfile);
              ssl_ctx_->use_private_key_file(keyfile,
                                             boost::asio::ssl::context::pem);
              ssl_ctx_->set_verify_mode(boost::asio::ssl::verify_none);
              tls_socket_.emplace(*socket_, *ssl_ctx_);
              auto server_context
                = boost::asio::ssl::stream<boost::asio::ip::tcp::socket>::server;
              tls_socket_->handshake(server_context, ec);
              if (ec) {
                return connection_rp_.deliver(caf::make_error(
                  ec::system_error,
                  fmt::format("TLS handshake failed: {}", ec.message())));
              }
            }
            return connection_rp_.deliver();
          }))
          .send(caf::actor_cast<caf::actor>(hdl));
      }
    });
    return connection_rp_;
  }

  auto write(chunk_ptr chunk) -> caf::result<void> {
    if (connection_rp_.pending()) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} cannot write while a connect "
                                         "request is pending",
                                         *self_));
    }
    if (write_rp_.pending()) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} cannot write while a write "
                                         "request is pending",
                                         *self_));
    }

    if (! is_connected_ && connect_params_) {
      // Hold the promise until reconnection completes
      write_rp_ = self_->make_response_promise<void>();
      pending_write_promises_.emplace_back(write_rp_, chunk);
      return write_rp_;
    }

    write_rp_ = self_->make_response_promise<void>();
    auto on_write = [this, chunk,
                     weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(self_)](
                      boost::system::error_code ec, size_t length) {
      if (auto hdl = weak_hdl.lock()) {
        caf::anon_mail(caf::make_action([this, chunk, ec, length] {
          auto bytes = as_bytes(chunk);
          auto sv = std::string_view(
            reinterpret_cast<const char*>(bytes.data()), bytes.size());
          TENZIR_INFO("callback {}", sv);
          if (ec) {
            if (is_disconnection_error(ec) && connect_params_) {
              auto bytes = as_bytes(chunk);
              auto sv = std::string_view(
                reinterpret_cast<const char*>(bytes.data()), bytes.size());
              TENZIR_INFO("tcp connector write detected disconnection: {}",
                          ec.message());
              pending_write_promises_.emplace_back(write_rp_, chunk);
              handle_disconnection();
              return;
            }
            write_rp_.deliver(caf::make_error(
              ec::system_error,
              fmt::format("failed to write to TCP socket: {}", ec.message())));
            return;
          }
          metrics_.writes++;
          metrics_.bytes_written += length;
          if (length < chunk->size()) {
            auto remainder = chunk->slice(length);
            write_rp_.delegate(static_cast<tcp_bridge_actor>(self_),
                               atom::write_v, std::move(remainder));
            return;
          }
          TENZIR_ASSERT(length == chunk->size());
          write_rp_.deliver();
        }))
          .send(caf::actor_cast<caf::actor>(hdl));
      }
    };
    auto asio_buffer = boost::asio::buffer(chunk->data(), chunk->size());
    if (tls_socket_) {
      tls_socket_->async_write_some(asio_buffer, on_write);
    } else {
      socket_->async_write_some(asio_buffer, on_write);
    }
    return write_rp_;
  }

  // Member variables
  tcp_bridge_actor::pointer self_ = {};
  // Retry logic members
  uint32_t max_retries_ = {};
  uint64_t initial_retry_delay_ms_ = {};
  double retry_backoff_multiplier_ = {};
  uint64_t max_retry_delay_ms_ = {};
  uint32_t current_retry_attempt_ = {};
  std::optional<connection_params> connect_params_ = {};
  std::optional<boost::asio::steady_timer> retry_timer_ = {};
  bool is_connected_ = {};
  std::vector<std::pair<caf::typed_response_promise<void>, chunk_ptr>>
    pending_write_promises_ = {};
  std::shared_ptr<boost::asio::io_context> io_ctx_ = {};
  std::thread worker_ = {};
  std::optional<boost::asio::ip::tcp::socket> socket_ = {};
  std::optional<boost::asio::ssl::context> ssl_ctx_ = {};
  std::optional<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>>
    tls_socket_ = {};
  std::optional<boost::asio::ip::tcp::acceptor> acceptor_ = {};
  caf::typed_response_promise<void> connection_rp_ = {};
  caf::typed_response_promise<void> write_rp_ = {};
  tcp_metrics metrics_ = {};
};

struct connector_args : ssl_options {
  std::string hostname = {};
  std::string port = {};
  bool listen_once = false;

  friend auto inspect(auto& f, connector_args& x) -> bool {
    return f.object(x).fields(
      f.field("ssl_options", static_cast<ssl_options&>(x)),
      f.field("hostname", x.hostname), f.field("port", x.port),
      f.field("listen_once", x.listen_once));
  }
};

struct saver_args : connector_args {
  bool listen = false;

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.tcp.saver_args")
      .fields(f.field("connector_args", static_cast<connector_args&>(x)),
              f.field("listen", x.listen));
  }
};

class save_tcp_operator final : public crtp_operator<save_tcp_operator> {
public:
  save_tcp_operator() = default;

  explicit save_tcp_operator(saver_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> bytes, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    if (args_.get_tls().inner and args_.listen) {
      // Verify that the files actually exist and are readable.
      // Ideally we'd also like to verify that the files contain valid
      // key material, but there's no straightforward API for this in
      // OpenSSL.
      TENZIR_ASSERT(args_.keyfile);
      TENZIR_ASSERT(args_.certfile);
      auto* keyfile = std::fopen(args_.keyfile->inner.c_str(), "r");
      if (not keyfile) {
        auto error = detail::describe_errno();
        diagnostic::error("failed to open TLS keyfile")
          .hint("{}", error)
          .emit(ctrl.diagnostics());
        co_return;
      }
      std::fclose(keyfile);
      auto* certfile = std::fopen(args_.certfile->inner.c_str(), "r");
      if (not certfile) {
        auto error = detail::describe_errno();
        diagnostic::error("failed to open TLS certfile")
          .hint("{}", error)
          .emit(ctrl.diagnostics());
        co_return;
      }
      std::fclose(certfile);
    }
    auto tcp_metrics = ctrl.metrics({
      "tenzir.metrics.tcp",
      record_type{
        {"handle", string_type{}},
        {"reads", uint64_type{}},
        {"writes", uint64_type{}},
        {"bytes_read", uint64_type{}},
        {"bytes_written", uint64_type{}},
      },
    });
    auto tcp_bridge = ctrl.self().spawn(caf::actor_from_state<class tcp_bridge>,
                                        std::move(tcp_metrics));
    if (not args_.listen) {
      auto cacert = args_.cacert.has_value()
                      ? args_.cacert->inner
                      : ssl_options::query_cacert_fallback(ctrl);
      auto certfile
        = args_.certfile.has_value() ? args_.certfile->inner : std::string{};
      auto keyfile
        = args_.keyfile.has_value() ? args_.keyfile->inner : std::string{};
      ctrl.self()
        .mail(atom::connect_v, args_.get_tls().inner, cacert, certfile, keyfile,
              args_.skip_peer_verification.has_value(), args_.hostname,
              args_.port)
        .request(tcp_bridge, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error("tcp saver failed to connect")
              .note("with error: {}", err)
              .note("while connecting to {}:{}", args_.hostname, args_.port)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    } else {
      ctrl.self()
        .mail(atom::accept_v, args_.hostname, args_.port,
              args_.certfile.value_or(located{std::string{}, location::unknown})
                .inner,
              args_.keyfile.value_or(located{std::string{}, location::unknown})
                .inner)
        .request(tcp_bridge, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error("failed to listen: {}", err)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
    for (auto chunk : bytes) {
      if (not chunk) {
        co_yield {};
        continue;
      }
      if (chunk->size() == 0) {
        continue;
      }
      ctrl.self()
        .mail(atom::write_v, std::move(chunk))
        .request(tcp_bridge, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error("tcp connector encountered error: {}", err)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "save_tcp";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, save_tcp_operator& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_));
  }

private:
  saver_args args_;
};

class save_tcp final : public virtual operator_plugin2<save_tcp_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = saver_args{};
    args.tls = located{false, inv.self.get_location()};
    auto parser = argument_parser2::operator_(name());
    auto uri = located<std::string>{};
    parser.positional("endpoint", uri, "string");
    args.add_tls_options(parser);
    TRY(parser.parse(inv, ctx));
    if (uri.inner.starts_with("tcp://")) {
      uri.inner.erase(0, 6);
    }
    TRY(args.validate(uri, ctx));
    auto split = detail::split(uri.inner, ":", 1);
    if (split.size() != 2) {
      diagnostic::error("malformed endpoint")
        .primary(uri.source)
        .hint("format must be 'tcp://address:port'")
        .emit(ctx);
      return failure::promise();
    }
    args.hostname = std::string{split[0]};
    args.port = std::string{split[1]};
    return std::make_unique<save_tcp_operator>(std::move(args));
  }

  auto save_properties() const -> save_properties_t override {
    return {
      .schemes = {"tcp"},
    };
  }
};

} // namespace

} // namespace tenzir::plugins::tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tcp::save_tcp)
