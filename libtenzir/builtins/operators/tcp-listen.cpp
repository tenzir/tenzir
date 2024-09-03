//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/config.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/location.hpp>
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

namespace tenzir::plugins::tcp_listen {

namespace {

using connection_actor = caf::typed_actor<auto(int)->caf::result<void>>;
using connection_manager_actor = caf::typed_actor<auto(int)->caf::result<void>>;

struct tcp_listen_args {
  std::string hostname = {};
  std::string port = {};
  bool connect = false;
  bool listen_once = false;
  bool tls = false;
  std::optional<std::string> tls_certfile = {};
  std::optional<std::string> tls_keyfile = {};
  operator_box op = {};
  bool no_location_overrides = false;
  bool has_terminal = false;
  bool is_hidden = false;

  friend auto inspect(auto& f, tcp_listen_args& x) -> bool {
    return f.object(x).fields(
      f.field("hostname", x.hostname), f.field("port", x.port),
      f.field("tls", x.tls), f.field("connect", x.connect),
      f.field("listen_once", x.listen_once),
      f.field("tls_certfile", x.tls_certfile),
      f.field("tls_keyfile", x.tls_keyfile), f.field("op", x.op),
      f.field("no_location_overrides", x.no_location_overrides),
      f.field("has_terminal", x.has_terminal),
      f.field("is_hidden", x.is_hidden));
  }
};

class tcp_listen_control_plane final : public operator_control_plane {
public:
  tcp_listen_control_plane(shared_diagnostic_handler diagnostics,
                           bool has_terminal, bool no_location_overrides,
                           bool is_hidden)
    : diagnostics_{std::move(diagnostics)},
      no_location_overrides_{no_location_overrides},
      has_terminal_{has_terminal},
      is_hidden_{is_hidden} {
  }

  auto self() noexcept -> exec_node_actor::base& override {
    TENZIR_UNIMPLEMENTED();
  }

  auto node() noexcept -> node_actor override {
    TENZIR_UNIMPLEMENTED();
  }

  auto diagnostics() noexcept -> diagnostic_handler& override {
    return diagnostics_;
  }

  auto metrics(type) noexcept -> metric_handler override {
    TENZIR_UNIMPLEMENTED();
  }

  auto no_location_overrides() const noexcept -> bool override {
    return no_location_overrides_;
  }

  auto has_terminal() const noexcept -> bool override {
    return has_terminal_;
  }

  auto is_hidden() const noexcept -> bool override {
    return is_hidden_;
  }

  auto set_waiting(bool value) noexcept -> void override {
    (void)value;
    TENZIR_UNIMPLEMENTED();
  }

private:
  shared_diagnostic_handler diagnostics_;
  bool no_location_overrides_;
  bool has_terminal_;
  bool is_hidden_;
};

using bridge_actor = caf::typed_actor<
  // Forwards slices from the connection actors to the operator
  auto(table_slice slice)->caf::result<void>,
  auto(atom::get)->caf::result<table_slice>>;

struct connection_state {
  static constexpr auto name = "tcp-listen-connection";

  ~connection_state() noexcept {
    // We ignore errors on shutdown. Just trying to close as much as possible
    // here.
    auto ec = boost::system::error_code{};
    if (tls_socket) {
      tls_socket->shutdown(ec);
      tls_socket->lowest_layer().shutdown(
        boost::asio::ip::tcp::socket::shutdown_both, ec);
      tls_socket->lowest_layer().cancel(ec);
      tls_socket->lowest_layer().close(ec);
    } else if (socket) {
      socket->shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
      socket->cancel(ec);
      socket->close(ec);
    }
  }

  connection_actor::pointer self = {};
  std::shared_ptr<boost::asio::io_context> io_context = {};
  std::optional<boost::asio::ip::tcp::socket> socket = {};
  std::optional<boost::asio::ssl::context> ssl_ctx = {};
  std::optional<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>>
    tls_socket = {};
  detail::weak_handle<bridge_actor> bridge;
  tcp_listen_args args;
  std::unique_ptr<operator_control_plane> ctrl;

  generator<table_slice> gen = {};
  generator<table_slice>::iterator it = {};
};

auto make_connection(connection_actor::stateful_pointer<connection_state> self,
                     std::shared_ptr<boost::asio::io_context> io_context,
                     boost::asio::ip::tcp::socket socket, bridge_actor bridge,
                     tcp_listen_args args,
                     shared_diagnostic_handler diagnostics)
  -> connection_actor::behavior_type {
  if (self->getf(caf::scheduled_actor::is_detached_flag)) {
    thread_local auto thread_name
      = fmt::format("tcp_fd{}", socket.native_handle());
    caf::detail::set_thread_name(thread_name.data());
  }
  self->state.self = self;
  self->state.io_context = std::move(io_context);
  self->state.socket = std::move(socket);
  self->state.bridge = std::move(bridge);
  self->state.args = std::move(args);
  self->state.ctrl = std::make_unique<tcp_listen_control_plane>(
    std::move(diagnostics), args.no_location_overrides, args.has_terminal,
    args.is_hidden);
  self->set_exception_handler(
    [self](std::exception_ptr exception) -> caf::error {
      try {
        std::rethrow_exception(exception);
      } catch (diagnostic diag) {
        self->state.ctrl->diagnostics().emit(std::move(diag));
        return {};
      } catch (const std::exception& err) {
        diagnostic::error("{}", err.what())
          .note("unhandled exception in {}", *self)
          .emit(self->state.ctrl->diagnostics());
        return {};
      }
      return diagnostic::error("unhandled exception in {}", *self).to_error();
    });
  if (self->state.args.tls) {
    self->state.ssl_ctx.emplace(boost::asio::ssl::context::tls_server);
    self->state.ssl_ctx->set_default_verify_paths();
    self->state.ssl_ctx->set_verify_mode(
      boost::asio::ssl::verify_peer
      | boost::asio::ssl::verify_fail_if_no_peer_cert);
    self->state.tls_socket.emplace(*self->state.socket, *self->state.ssl_ctx);
    auto tls_handle = self->state.tls_socket->native_handle();
    if (SSL_set1_host(tls_handle, self->state.args.hostname.c_str()) != 1) {
      diagnostic::error("failed to enable host name verification")
        .emit(self->state.ctrl->diagnostics());
      return connection_actor::behavior_type::make_empty_behavior();
    }
    if (not SSL_set_tlsext_host_name(tls_handle,
                                     self->state.args.hostname.c_str())) {
      diagnostic::error("failed to set SNI")
        .emit(self->state.ctrl->diagnostics());
      return connection_actor::behavior_type::make_empty_behavior();
    }
    if (self->state.args.tls_certfile) {
      self->state.ssl_ctx->use_certificate_chain_file(
        *self->state.args.tls_certfile);
    }
    if (self->state.args.tls_keyfile) {
      self->state.ssl_ctx->use_private_key_file(*self->state.args.tls_keyfile,
                                                boost::asio::ssl::context::pem);
    }
    self->state.ssl_ctx->set_verify_mode(boost::asio::ssl::verify_none);
    self->state.tls_socket.emplace(*self->state.socket, *self->state.ssl_ctx);
    auto server_context
      = boost::asio::ssl::stream<boost::asio::ip::tcp::socket>::server;
    auto ec = boost::system::error_code{};
    self->state.tls_socket->handshake(server_context, ec);
    if (ec) {
      diagnostic::warning("{}", ec.message())
        .note("TLS handshake failed")
        .emit(self->state.ctrl->diagnostics());
      return connection_actor::behavior_type::make_empty_behavior();
    }
  }
  auto input = [](connection_state& state) -> generator<chunk_ptr> {
    auto buffer = std::array<char, 65'536>{};
    auto ec = boost::system::error_code{};
    while (true) {
      const auto length
        = state.tls_socket
            ? state.tls_socket->read_some(boost::asio::buffer(buffer), ec)
            : state.socket->read_some(boost::asio::buffer(buffer), ec);
      if (ec == boost::asio::error::eof) {
        TENZIR_ASSERT(length == 0);
        co_return;
      }
      if (ec) {
        diagnostic::error("{}", ec.message())
          .note("failed to read from socket")
          .emit(state.ctrl->diagnostics());
        co_return;
      }
      co_yield chunk::copy(as_bytes(buffer).subspan(0, length));
    }
  }(self->state);
  auto gen
    = self->state.args.op->instantiate(std::move(input), *self->state.ctrl);
  if (not gen) {
    diagnostic::error(gen.error()).emit(self->state.ctrl->diagnostics());
    return connection_actor::behavior_type::make_empty_behavior();
  }
  auto* typed_gen = std::get_if<generator<table_slice>>(&*gen);
  TENZIR_ASSERT(typed_gen);
  self->state.gen = std::move(*typed_gen);
  self->state.it = self->state.gen.begin();
  detail::weak_run_delayed_loop(self, duration::zero(), [self] {
    if (self->state.it == self->state.gen.end()) {
      self->quit();
      return;
    }
    auto slice = std::move(*self->state.it);
    {
      auto handle = self->state.bridge.lock();
      if (not handle) {
        self->quit();
        return;
      }
      // Using self->request here would internally hold a strong handle on the
      // bridge, and would keep it alive that way until a response comes back.
      // This becomes a problem when multiple connections are present while the
      // operator terminates. If the windows in which the connections relinquish
      // their handles don't overlap the bridge and all connections are kept
      // alive indefinitely.
      caf::anon_send(handle, std::move(slice));
    }
    ++self->state.it;
  });
  return {
    [](int) {
      // dummy because no behavior means quitting
    },
  };
}

struct connection_manager_state {
  static constexpr auto name = "tcp-listen-connection-manager";

  connection_manager_actor::pointer self = {};
  detail::weak_handle<bridge_actor> bridge = {};
  tcp_listen_args args = {};
  shared_diagnostic_handler diagnostics = {};

  std::shared_ptr<boost::asio::io_context> io_context = {};
  std::optional<boost::asio::ip::tcp::socket> socket = {};
  std::optional<boost::asio::ip::tcp::endpoint> endpoint = {};
  std::optional<boost::asio::ip::tcp::acceptor> acceptor = {};

  std::vector<connection_actor> connections = {};

  auto tcp_listen() {
    acceptor->async_accept([this](boost::system::error_code ec,
                                  boost::asio::ip::tcp::socket socket) {
      if (ec) {
        diagnostic::error("{}", ec.message())
          .note("failed to tcp_listen connection")
          .throw_();
      }
#if TENZIR_MACOS
      if (::fcntl(socket.native_handle(), F_SETFD, FD_CLOEXEC) == -1) {
        diagnostic::error("{}", detail::describe_errno())
          .note("failed to configure socket")
          .throw_();
      }
#endif
      auto handle = bridge.lock();
      if (not handle) {
        self->quit();
        return;
      }
      connections.push_back(self->spawn<caf::linked + caf::detached>(
        make_connection, io_context, std::move(socket), std::move(handle), args,
        diagnostics));
    });
    run();
  }

  auto run() -> void {
    detail::weak_run_delayed(self, duration::zero(), [this] {
      const auto num_runs = [&] {
        auto guard = boost::asio::make_work_guard(io_context);
        return io_context->run_one_for(std::chrono::milliseconds{500});
      }();
      if (num_runs == 0) {
        run();
        return;
      }
      TENZIR_ASSERT(num_runs == 1);
      io_context->restart();
      tcp_listen();
    });
  }
};

auto make_connection_manager(
  connection_manager_actor::stateful_pointer<connection_manager_state> self,
  bridge_actor bridge, tcp_listen_args args,
  shared_diagnostic_handler diagnostics)
  -> connection_manager_actor::behavior_type {
  self->state.self = self;
  self->state.io_context = std::make_shared<boost::asio::io_context>();
  self->state.bridge = std::move(bridge);
  self->state.args = std::move(args);
  self->state.diagnostics = std::move(diagnostics);
  self->set_exception_handler(
    [self](std::exception_ptr exception) -> caf::error {
      try {
        std::rethrow_exception(exception);
      } catch (const std::exception& err) {
        diagnostic::error("{}", err.what())
          .note("unhandled exception in {}", *self)
          .emit(self->state.diagnostics);
        return {};
      }
      return diagnostic::error("unhandled exception in {}", *self).to_error();
    });
  auto resolver = boost::asio::ip::tcp::resolver{*self->state.io_context};
  auto endpoints
    = resolver.resolve(self->state.args.hostname, self->state.args.port);
  if (endpoints.empty()) {
    diagnostic::error("failed to resolve {}:{}", self->state.args.hostname,
                      self->state.args.port)
      .emit(self->state.diagnostics);
    return connection_manager_actor::behavior_type::make_empty_behavior();
  }
  self->state.endpoint = endpoints.begin()->endpoint();
  self->state.acceptor = boost::asio::ip::tcp::acceptor(*self->state.io_context,
                                                        *self->state.endpoint);
  auto reuse_address = boost::asio::ip::tcp::acceptor::reuse_address{true};
  self->state.acceptor->set_option(reuse_address);
  self->state.acceptor->listen(boost::asio::socket_base::max_connections);
  self->state.socket = boost::asio::ip::tcp::socket(*self->state.io_context);
#if TENZIR_LINUX
  auto sfd = ::socket(self->state.endpoint->protocol().family(),
                      SOCK_STREAM | SOCK_CLOEXEC,
                      self->state.endpoint->protocol().protocol());
  TENZIR_ASSERT(sfd >= 0);
  int opt = 1;
  if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int)) < 0) {
    diagnostic::error("failed to configure socket {}:{}: {}",
                      self->state.args.hostname, self->state.args.port,
                      detail::describe_errno())
      .emit(self->state.diagnostics);
    return connection_manager_actor::behavior_type::make_empty_behavior();
  }
  self->state.socket->assign(self->state.endpoint->protocol(), sfd);
#endif
  self->state.tcp_listen();
  return {
    [](int) {
      // dummy because no behavior means quitting
    },
  };
}

struct bridge_state {
  std::queue<table_slice> buffer;
  caf::typed_response_promise<table_slice> buffer_rp;

  connection_manager_actor connection_manager = {};
};

auto make_bridge(bridge_actor::stateful_pointer<bridge_state> self,
                 tcp_listen_args args, shared_diagnostic_handler diagnostics)
  -> bridge_actor::behavior_type {
  self->state.connection_manager = self->spawn<caf::linked + caf::detached>(
    make_connection_manager, bridge_actor{self}, std::move(args),
    std::move(diagnostics));
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

class tcp_listen_operator final : public crtp_operator<tcp_listen_operator> {
public:
  tcp_listen_operator() = default;

  explicit tcp_listen_operator(tcp_listen_args args) : args_{std::move(args)} {
    // nop
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto args = args_;
    args.no_location_overrides = ctrl.no_location_overrides();
    args.has_terminal = ctrl.has_terminal();
    args.is_hidden = ctrl.is_hidden();
    auto bridge = ctrl.self().spawn<caf::linked>(make_bridge, std::move(args),
                                                 ctrl.shared_diagnostics());
    while (true) {
      auto slice = table_slice{};
      ctrl.set_waiting(true);
      ctrl.self()
        .request(bridge, caf::infinite, atom::get_v)
        .then(
          [&](table_slice& result) {
            ctrl.set_waiting(false);
            slice = std::move(result);
          },
          [&](const caf::error& err) {
            diagnostic::error(err).emit(ctrl.diagnostics());
          });
      co_yield {};
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "tcp-listen";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    auto result = args_.op->optimize(filter, order);
    if (not result.replacement) {
      return result;
    }
    TENZIR_ASSERT(not dynamic_cast<pipeline*>(result.replacement.get()));
    auto args = args_;
    args.op = std::move(result.replacement);
    result.replacement = std::make_unique<tcp_listen_operator>(std::move(args));
    return result;
  }

  friend auto inspect(auto& f, tcp_listen_operator& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_));
  }

private:
  tcp_listen_args args_ = {};
};

class plugin final : public virtual operator_plugin<tcp_listen_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = false,
      .sink = false,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    // tcp_listen <endpoint> [<args...>] read [<op_args...>]
    auto parser
      = argument_parser{"tcp-listen", "https://docs.tenzir.com/connectors/tcp"};
    auto q = until_keyword_parser{"read", p};
    auto args = tcp_listen_args{};
    auto endpoint = located<std::string>{};
    parser.add(endpoint, "<endpoint>");
    parser.add("-c,--connect", args.connect);
    parser.add("-o,--listen-once", args.listen_once);
    parser.add("--tls", args.tls);
    parser.add("--certfile", args.tls_certfile, "<TLS certificate>");
    parser.add("--keyfile", args.tls_keyfile, "<TLS private key>");
    parser.parse(q);
    if (endpoint.inner.starts_with("tcp://")) {
      endpoint.inner = std::move(endpoint.inner).substr(6);
    }
    const auto split = detail::split(endpoint.inner, ":", 1);
    if (split.size() != 2) {
      diagnostic::error("malformed endpoint")
        .primary(endpoint.source)
        .hint("format must be 'tcp://address:port'")
        .throw_();
    } else {
      args.hostname = std::string{split[0]};
      args.port = std::string{split[1]};
    }
    auto op_name = p.accept_identifier();
    if (op_name) {
      if (*op_name != "read") {
        diagnostic::error("expected `read`").primary(p.current_span()).throw_();
      }
      const auto* read_plugin = plugins::find_operator(op_name->name);
      if (not read_plugin) {
        diagnostic::error("operator `{}` does not exist", op_name->name)
          .primary(op_name->source)
          .throw_();
      }
      args.op = read_plugin->parse_operator(p);
    } else {
      auto read_pipe = pipeline::internal_parse("read json");
      if (not read_pipe) {
        diagnostic::error("failed to parse default format `json`")
          .primary(p.current_span())
          .throw_();
      }
      auto ops = std::move(*read_pipe).unwrap();
      TENZIR_ASSERT(ops.size() == 1);
      args.op = std::move(ops[0]);
    }
    TENZIR_ASSERT(args.op);
    TENZIR_ASSERT(not dynamic_cast<pipeline*>(args.op.get()));
    if (const auto ok = args.op->check_type<chunk_ptr, table_slice>(); not ok) {
      diagnostic::error(ok.error()).throw_();
    }
    // If connect or listen-once are specified, we fall back to the TCP lodaer.
    // This is obviously a hack, but we don't have a better solution for this
    // for now. Similarly, `from tcp` will dispatch to this undocumented
    // `tcp-listen` operator under the hood to allow multiple parallel
    // connections to be accepted, which the connector API cannot handle.
    if (args.connect or args.listen_once) {
      const auto load_definition = fmt::format(
        "load tcp {}:{} {}{}{}{}{}", args.hostname, args.port,
        args.connect ? " --connect" : "",
        args.listen_once ? " --listen-once" : "", args.tls ? " --tls" : "",
        args.tls_certfile ? fmt::format(" --certfile {}", args.tls_certfile)
                          : "",
        args.tls_keyfile ? fmt::format(" --keyfile {}", args.tls_keyfile) : "");
      auto load_read = pipeline::internal_parse(load_definition);
      TENZIR_ASSERT(load_read);
      if (not load_read) {
        diagnostic::warning("`{}` failed to parse: {}", load_definition,
                            load_read.error())
          .throw_();
      }
      load_read->append(std::move(args.op));
      return std::make_unique<pipeline>(std::move(*load_read));
    }
    return std::make_unique<tcp_listen_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::tcp_listen

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tcp_listen::plugin)
