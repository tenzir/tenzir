//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/config.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/location.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <algorithm>
#include <iterator>
#include <memory>
#include <netdb.h>
#include <queue>
#include <utility>

namespace tenzir::plugins::load_tcp {

namespace {

// -- actor interfaces ---------------------------------------------------------

using connection_actor = caf::typed_actor<
  // Read bytes from a connection buffer.
  auto(atom::read, boost::asio::ip::tcp::socket::native_handle_type handle)
    ->caf::result<chunk_ptr>>;

template <class Elements>
using connection_manager_actor = caf::typed_actor<
  // Write elements into the buffer.
  auto(atom::write, Elements elements)->caf::result<void>,
  // Read elements from the buffer.
  auto(atom::read)->caf::result<Elements>>
  // Support reading from a connection.
  ::template extend_with<connection_actor>
  // Handle metrics of the nested pipelines.
  ::template extend_with<metrics_receiver_actor>
  // Handle diagnostics of the nested pipelines.
  ::template extend_with<receiver_actor<diagnostic>>;

// -- helper structs -----------------------------------------------------------

struct endpoint {
  std::string hostname = {};
  std::string port = {};

  friend auto inspect(auto& f, endpoint& x) -> bool {
    return f.object(x).fields(f.field("hostname", x.hostname),
                              f.field("port", x.port));
  }
};

struct load_tcp_args {
  located<struct endpoint> endpoint = {};
  located<uint64_t> parallel = {};
  std::optional<location> connect = {};
  std::optional<location> tls = {};
  std::optional<located<std::string>> certfile = {};
  std::optional<located<std::string>> keyfile = {};
  std::optional<located<class pipeline>> pipeline = {};

  friend auto inspect(auto& f, load_tcp_args& x) -> bool {
    return f.object(x).fields(
      f.field("endpoint", x.endpoint), f.field("parallel", x.parallel),
      f.field("connect", x.connect), f.field("tls", x.tls),
      f.field("certfile", x.certfile), f.field("keyfile", x.keyfile),
      f.field("pipeline", x.pipeline));
  }
};

// -- helper functions ---------------------------------------------------------

auto set_close_on_exec(boost::asio::ip::tcp::socket::native_handle_type handle)
  -> caf::expected<void> {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  if (::fcntl(handle, F_SETFD, FD_CLOEXEC) != 0) {
    return diagnostic::error("{}", detail::describe_errno())
      .note("failed to configure socket to close on exec")
      .to_error();
  }
  return {};
}

auto resolve_endpoint(boost::asio::io_context& io_ctx,
                      const located<struct endpoint>& endpoint)
  -> caf::expected<boost::asio::ip::tcp::endpoint> {
  auto ec = boost::system::error_code{};
  auto resolver = boost::asio::ip::tcp::resolver{io_ctx};
  auto endpoints
    = resolver.resolve(endpoint.inner.hostname, endpoint.inner.port, ec);
  if (ec) {
    return diagnostic::error("{}", ec.message())
      .note("failed to resolve endpoint")
      .primary(endpoint.source)
      .to_error();
  }
  if (endpoints.empty()) {
    return diagnostic::error("no endpoints found")
      .primary(endpoint.source)
      .to_error();
  }
  return *endpoints.begin();
}

// -- load_tcp_source operator -------------------------------------------------

class load_tcp_source_operator final
  : public crtp_operator<load_tcp_source_operator> {
public:
  load_tcp_source_operator() = default;

  explicit load_tcp_source_operator(
    const connection_actor& connection,
    boost::asio::ip::tcp::socket::native_handle_type handle)
    : connection_{connection}, handle_{handle} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto connection = connection_.lock();
    TENZIR_ASSERT(connection);
    while (true) {
      auto result = chunk_ptr{};
      ctrl.set_waiting(true);
      ctrl.self()
        .request(connection, caf::infinite, atom::read_v, handle_)
        .then(
          [&](chunk_ptr chunk) {
            ctrl.set_waiting(false);
            result = std::move(chunk);
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to read from TCP connection")
              .note("handle `{}`", handle_)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      if (size(result) == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto name() const -> std::string override {
    return "internal-load-tcp-source-bytes";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    TENZIR_ASSERT(input.is<void>());
    return tag_v<chunk_ptr>;
  }

  friend auto inspect(auto& f, load_tcp_source_operator& x) -> bool {
    return f.object(x).fields(f.field("connection", x.connection_),
                              f.field("handle", x.handle_));
  }

private:
  detail::weak_handle<connection_actor> connection_ = {};
  boost::asio::ip::tcp::socket::native_handle_type handle_ = {};
};

// -- load_tcp_sink operator ---------------------------------------------------

template <class Elements>
class load_tcp_sink_operator final
  : public crtp_operator<load_tcp_sink_operator<Elements>> {
public:
  load_tcp_sink_operator() = default;

  explicit load_tcp_sink_operator(
    const connection_manager_actor<Elements>& connection_manager)
    : connection_manager_{connection_manager} {
  }

  auto operator()(generator<Elements> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto connection_manager = connection_manager_.lock();
    TENZIR_ASSERT(connection_manager);
    for (auto&& elements : input) {
      ctrl.set_waiting(true);
      ctrl.self()
        .request(connection_manager, caf::infinite, atom::write_v,
                 std::move(elements))
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to read from TCP connection")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
  }

  auto location() const -> operator_location override {
    return operator_location::anywhere;
  }

  auto name() const -> std::string override {
    return fmt::format("internal-load-tcp-sink-{}",
                       operator_type_name<Elements>());
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    TENZIR_ASSERT(input.is<Elements>());
    return tag_v<void>;
  }

  friend auto inspect(auto& f, load_tcp_sink_operator& x) -> bool {
    return f.apply(x.connection_manager_);
  }

private:
  detail::weak_handle<connection_manager_actor<Elements>> connection_manager_
    = {};
};

// -- connection-manager actor -------------------------------------------------

template <class Elements>
struct connection_manager_state {
  [[maybe_unused]] static constexpr auto name = "connection-manager";

  connection_manager_actor<Elements>::pointer self = {};
  load_tcp_args args = {};
  shared_diagnostic_handler diagnostics = {};
  metrics_receiver_actor metrics_receiver = {};
  detail::stable_map<uint64_t, detail::stable_map<uint64_t, uint64_t>>
    metrics_id_map = {};
  static constexpr auto tcp_metrics_id = uint64_t{0};
  uint64_t next_metrics_id = tcp_metrics_id + 1;
  uint64_t operator_id = {};

  // Everything required for the I/O worker.
  std::vector<std::thread> io_workers = {};
  std::shared_ptr<boost::asio::io_context> io_ctx = {};
  std::optional<boost::asio::ip::tcp::socket> socket = {};

  // Everything required for listening for connections.
  std::optional<boost::asio::ip::tcp::acceptor> acceptor = {};

  // Everything needed for back pressure handling.
  static constexpr auto max_buffered_batches = size_t{20};
  std::queue<Elements> buffer = {};
  caf::typed_response_promise<Elements> read_rp = {};
  std::queue<caf::typed_response_promise<void>> write_rps = {};

  // State we need to keep for each peer.
  struct connection_state : std::enable_shared_from_this<connection_state> {
    connection_state() = default;
    connection_state(const connection_state&) = delete;
    connection_state(connection_state&&) = delete;
    auto operator=(const connection_state&) -> connection_state& = delete;
    auto operator=(connection_state&&) -> connection_state& = delete;

    ~connection_state() noexcept {
      next_emit_metrics.dispose();
      emit_metrics(nullptr);
    }

    static constexpr auto max_queued_chunks = size_t{10};
    static constexpr auto read_buffer_size = size_t{65'536};

    std::optional<boost::asio::ip::tcp::socket> socket = {};
    std::optional<boost::asio::ssl::context> ssl_ctx = {};
    std::optional<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>>
      tls_socket = {};
    pipeline_executor_actor pipeline_executor = {};

    // The mutex is protecting the queue of chunks and the response promise, as
    // they're both used from the Asio-managed thread pool.
    mutable std::mutex mutex = {};
    std::queue<chunk_ptr> chunks = {};
    caf::typed_response_promise<chunk_ptr> rp = {};

    metrics_receiver_actor metrics_receiver = {};
    uint64_t operator_id = {};
    uint64_t reads = {};
    uint64_t bytes_read = {};
    caf::disposable next_emit_metrics = {};

    auto emit_metrics(connection_manager_actor<Elements>::pointer self)
      -> void {
      auto metric = record{
        {"timestamp", time{time::clock::now()}},
        {"handle", fmt::to_string(socket->native_handle())},
        {"reads", std::exchange(reads, {})},
        {"writes", uint64_t{0}},
        {"bytes_read", std::exchange(bytes_read, {})},
        {"bytes_written", uint64_t{0}},
      };
      caf::anon_send(metrics_receiver, operator_id, tcp_metrics_id,
                     std::move(metric));
      if (self) {
        next_emit_metrics
          = detail::weak_run_delayed(self, defaults::metrics_interval,
                                     [self, weak_ptr = this->weak_from_this()] {
                                       if (auto connection = weak_ptr.lock()) {
                                         connection->emit_metrics(self);
                                       }
                                     });
      }
    }

    auto async_read(connection_manager_actor<Elements>::pointer self,
                    shared_diagnostic_handler diagnostics) -> void {
      // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays)
      auto read_buffer
        = std::make_unique<chunk::value_type[]>(read_buffer_size);
      // NOLINTEND(cppcoreguidelines-avoid-c-arrays)
      auto asio_buffer
        = boost::asio::buffer(read_buffer.get(), read_buffer_size);
      auto on_read = [connection = this->shared_from_this(), self,
                      diagnostics = std::move(diagnostics),
                      read_buffer = std::move(read_buffer)](
                       boost::system::error_code ec, size_t length) mutable {
        connection->reads += 1;
        connection->bytes_read += length;
        if (ec) {
          // We intentionally pass the empty chunk to the nested pipeline's
          // source to let that shut down cleanly.
          TENZIR_ASSERT(length == 0);
          if (ec != boost::asio::error::eof) {
            diagnostic::warning("{}", ec.message())
              .note("failed to read from TCP connection")
              .note("handle `{}`", connection->socket->native_handle())
              .emit(diagnostics);
          }
        } else {
          TENZIR_ASSERT(length > 0);
        }
        const auto* data = read_buffer.get();
        auto chunk = chunk::make(
          data, length, [read_buffer = std::move(read_buffer)]() noexcept {
            (void)read_buffer;
          });
        auto should_read = false;
        {
          auto lock = std::unique_lock{connection->mutex};
          if (connection->rp.pending()) {
            caf::anon_send(caf::actor_cast<caf::actor>(self),
                           caf::make_action(
                             [self, connection, ec, chunk = std::move(chunk),
                              diagnostics = std::move(diagnostics)]() mutable {
                               auto lock = std::unique_lock{connection->mutex};
                               TENZIR_ASSERT(connection->rp.pending());
                               connection->rp.deliver(std::move(chunk));
                               if (not ec) {
                                 connection->async_read(self,
                                                        std::move(diagnostics));
                               }
                             }));
            TENZIR_ASSERT(connection->chunks.empty());
            return;
          }
          connection->chunks.push(std::move(chunk));
          TENZIR_ASSERT(connection->chunks.size() <= max_queued_chunks);
          should_read = connection->chunks.size() < max_queued_chunks;
        }
        if (not ec and should_read) {
          connection->async_read(self, std::move(diagnostics));
        }
      };
      if (tls_socket) {
        tls_socket->async_read_some(asio_buffer, std::move(on_read));
      } else {
        socket->async_read_some(asio_buffer, std::move(on_read));
      }
    }
  };
  std::unordered_map<boost::asio::ip::tcp::socket::native_handle_type,
                     std::shared_ptr<connection_state>>
    connections = {};

  // Everything required for spawning the nested pipeline.
  static constexpr auto has_terminal = false;
  bool is_hidden = {};
  node_actor node = {};

  connection_manager_state() = default;
  connection_manager_state(const connection_manager_state&) = delete;
  connection_manager_state(connection_manager_state&&) = delete;
  auto operator=(const connection_manager_state&)
    -> connection_manager_state& = delete;
  auto operator=(connection_manager_state&&)
    -> connection_manager_state& = delete;

  ~connection_manager_state() noexcept {
    io_ctx->stop();
    for (auto& io_worker : io_workers) {
      io_worker.join();
    }
  }

  auto start() -> caf::expected<void> {
    auto tcp_metrics_schema = type{
      "tenzir.metrics.tcp",
      record_type{
        {"handle", string_type{}},
        {"reads", uint64_type{}},
        {"writes", uint64_type{}},
        {"bytes_read", uint64_type{}},
        {"bytes_written", uint64_type{}},
      },
    };
    self
      ->request(metrics_receiver, caf::infinite, operator_id, tcp_metrics_id,
                std::move(tcp_metrics_schema))
      .then([]() {},
            [this](const caf::error& err) {
              diagnostic::error(err)
                .note("failed to register TCP metrics schema")
                .emit(diagnostics);
            });
    TENZIR_ASSERT(not io_ctx);
    io_ctx = std::make_shared<boost::asio::io_context>();
    io_workers.reserve(args.parallel.inner);
    std::ranges::generate_n(
      std::back_inserter(io_workers), args.parallel.inner, [this] {
        return std::thread{[this] {
          const auto guard = boost::asio::make_work_guard(*io_ctx);
          io_ctx->run();
        }};
      });
    socket.emplace(*io_ctx);
    return args.connect ? connect() : listen();
  }

  auto connect() -> caf::expected<void> {
    TENZIR_ASSERT(args.connect);
    // TODO: Implement support for connect=true.
    TENZIR_UNIMPLEMENTED();
    return {};
  }

  auto listen() -> caf::expected<void> {
    TENZIR_ASSERT(not args.connect);
    TENZIR_ASSERT(not acceptor);
    TRY(auto endpoint, resolve_endpoint(*io_ctx, args.endpoint));
    acceptor.emplace(*io_ctx);
    auto ec = boost::system::error_code{};
    if (acceptor->open(endpoint.protocol(), ec)) {
      return diagnostic::error("{}", ec.message())
        .note("failed to open acceptor")
        .primary(args.endpoint.source)
        .to_error();
    }
    TRY(set_close_on_exec(acceptor->native_handle()));
    if (acceptor->set_option(boost::asio::socket_base::reuse_address{true},
                             ec)) {
      return diagnostic::error("{}", ec.message())
        .note("failed to enable reuse address")
        .primary(args.endpoint.source)
        .to_error();
    }
    if (acceptor->bind(endpoint, ec)) {
      return diagnostic::error("{}", ec.message())
        .note("failed to bind to endpoint")
        .primary(args.endpoint.source)
        .to_error();
    }
    if (acceptor->listen(boost::asio::socket_base::max_connections, ec)) {
      return diagnostic::error("{}", ec.message())
        .note("failed to start listening")
        .primary(args.endpoint.source)
        .to_error();
    }
    async_accept();
    return {};
  }

  auto handle_connection(boost::asio::ip::tcp::socket peer) -> void {
    TENZIR_ASSERT(not connections.contains(peer.native_handle()));
    auto& connection = connections[peer.native_handle()];
    connection = std::make_shared<connection_state>();
    TENZIR_ASSERT(not connection->socket);
    connection->socket.emplace(std::move(peer));
    if (auto ok = set_close_on_exec(connection->socket->native_handle());
        not ok) {
      diagnostic::warning(ok.error())
        .note("handle `{}`", connection->socket->native_handle())
        .emit(diagnostics);
      return;
    }
    connection->metrics_receiver = metrics_receiver;
    connection->operator_id = operator_id;
    connection->emit_metrics(self);
    if (args.tls) {
      TENZIR_ASSERT(not connection->ssl_ctx);
      connection->ssl_ctx.emplace(boost::asio::ssl::context::tls_server);
      auto ec = boost::system::error_code{};
      if (args.certfile) {
        if (connection->ssl_ctx->use_certificate_chain_file(
              args.certfile->inner, ec)) {
          diagnostic::warning("{}", ec.message())
            .note("failed to load certificate chain file")
            .note("handle `{}`", connection->socket->native_handle())
            .primary(args.certfile->source)
            .emit(diagnostics);
          return;
        }
      }
      if (args.keyfile) {
        if (connection->ssl_ctx->use_private_key_file(
              args.keyfile->inner, boost::asio::ssl::context::pem, ec)) {
          diagnostic::warning("{}", ec.message())
            .note("failed to load private key file")
            .note("handle `{}`", connection->socket->native_handle())
            .primary(args.certfile->source)
            .emit(diagnostics);
          return;
        }
      }
      if (connection->ssl_ctx->set_verify_mode(boost::asio::ssl::verify_none,
                                               ec)) {
        diagnostic::warning("{}", ec.message())
          .note("failed to disable peer certificate verification")
          .note("handle `{}`", connection->socket->native_handle())
          .primary(*args.tls)
          .emit(diagnostics);
        return;
      }
      TENZIR_ASSERT(not connection->tls_socket);
      connection->tls_socket.emplace(*connection->socket, *connection->ssl_ctx);
      if (connection->tls_socket->handshake(
            boost::asio::ssl::stream<boost::asio::ip::tcp::socket>::server,
            ec)) {
        diagnostic::warning("{}", ec.message())
          .note("failed to perform TLS handshake")
          .note("handle `{}`", connection->socket->native_handle())
          .primary(*args.tls)
          .emit(diagnostics);
        return;
      }
    }
    // Set up and spawn the nested pipeline.
    auto pipeline = args.pipeline->inner;
    auto source = std::make_unique<load_tcp_source_operator>(
      connection_actor{self}, connection->socket->native_handle());
    pipeline.prepend(std::move(source));
    auto sink = std::make_unique<load_tcp_sink_operator<Elements>>(
      connection_manager_actor<Elements>{self});
    pipeline.append(std::move(sink));
    TENZIR_ASSERT(pipeline.is_closed());
    TENZIR_ASSERT(not connection->pipeline_executor);
    connection->pipeline_executor = self->template spawn<caf::monitored>(
      pipeline_executor, std::move(pipeline), receiver_actor<diagnostic>{self},
      metrics_receiver_actor{self}, node, has_terminal, is_hidden);
    if (std::is_same_v<Elements, chunk_ptr> and connections.size() > 1) {
      diagnostic::warning(
        "potentially interleaved bytes from parallel connections")
        .hint(args.pipeline->source == location::unknown
                ? "consider adding a nested pipeline that returns events"
                : "consider changing the nested pipeline to return events")
        .primary(args.pipeline->source)
        .emit(diagnostics);
    }
    self->request(connection->pipeline_executor, caf::infinite, atom::start_v)
      .then(
        [this, handle = connection->socket->native_handle()]() {
          // Start the async read loop for this connection.
          auto connection = connections.find(handle);
          TENZIR_ASSERT(connection != connections.end());
          connection->second->async_read(self, diagnostics);
        },
        [this, handle
               = connection->socket->native_handle()](const caf::error& err) {
          diagnostic::warning(err)
            .note("failed to start nested pipeline")
            .note("handle `{}`", handle)
            .primary(args.pipeline->source)
            .emit(diagnostics);
        });
  }

  auto async_accept() -> void {
    acceptor->async_accept(
      [this, handle = caf::actor_cast<caf::actor>(self)](
        boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
        auto action = [this, ec, socket = std::move(socket)]() mutable {
          // Always start accepting the next connection.
          async_accept();
          // If there's an error accepting connections, then we just warn about
          // it but continue to accept new ones.
          if (ec) {
            diagnostic::warning("{}", ec.message())
              .note("failed to accept connection")
              .primary(args.endpoint.source)
              .emit(diagnostics);
            return;
          }
          handle_connection(std::move(socket));
        };
        caf::anon_send(handle, caf::make_action(std::move(action)));
      });
  }

  auto
  read_from_connection(boost::asio::ip::tcp::socket::native_handle_type handle)
    -> caf::result<chunk_ptr> {
    auto connection = connections.find(handle);
    auto chunk = chunk_ptr{};
    if (connection == connections.end()) {
      return chunk;
    }
    auto should_read = false;
    {
      auto lock = std::unique_lock{connection->second->mutex};
      if (connection->second->chunks.empty()) {
        TENZIR_ASSERT(not connection->second->rp.pending());
        connection->second->rp
          = self->template make_response_promise<chunk_ptr>();
        return connection->second->rp;
      }
      should_read = connection->second->chunks.size()
                    == connection_state::max_queued_chunks;
      chunk = std::move(connection->second->chunks.front());
      connection->second->chunks.pop();
    }
    if (should_read) {
      connection->second->async_read(self, diagnostics);
    }
    return chunk;
  }

  auto write_elements(Elements elements) -> caf::result<void> {
    if (read_rp.pending()) {
      TENZIR_ASSERT(buffer.empty());
      read_rp.deliver(std::move(elements));
    }
    buffer.push(std::move(elements));
    if (buffer.size() < max_buffered_batches) {
      return {};
    }
    return write_rps.emplace(self->template make_response_promise<void>());
  }

  auto read_elements() -> caf::result<Elements> {
    TENZIR_ASSERT(not read_rp.pending());
    if (not buffer.empty()) {
      auto elements = std::move(buffer.front());
      buffer.pop();
      if (buffer.size() < max_buffered_batches) {
        // Unblock all connections as soon as at least one free slot in the
        // buffer opens up.
        while (not write_rps.empty()) {
          auto write_rp = std::move(write_rps.front());
          write_rps.pop();
          TENZIR_ASSERT(write_rp.pending());
          write_rp.deliver();
        }
      }
      return elements;
    }
    read_rp = self->template make_response_promise<Elements>();
    return read_rp;
  }

  auto handle_down_msg(const caf::down_msg& msg) -> void {
    const auto connection
      = std::ranges::find_if(connections, [&](const auto& connection) {
          return connection.second->pipeline_executor.address() == msg.source;
        });
    TENZIR_ASSERT(connection != connections.end());
    if (msg.reason) {
      diagnostic::warning(msg.reason)
        .note("nested pipeline terminated unexpectedly")
        .note("handle `{}`", connection->first)
        .primary(args.pipeline->source)
        .emit(diagnostics);
    }
    connections.erase(connection);
  }
};

template <class Elements>
auto make_connection_manager(
  typename connection_manager_actor<Elements>::template stateful_pointer<
    connection_manager_state<Elements>>
    self,
  const load_tcp_args& args, const shared_diagnostic_handler& diagnostics,
  const metrics_receiver_actor& metrics_receiver, uint64_t operator_id,
  bool is_hidden, const node_actor& node)
  -> connection_manager_actor<Elements>::behavior_type {
  self->state.self = self;
  self->state.args = args;
  self->state.diagnostics = diagnostics;
  self->state.metrics_receiver = metrics_receiver;
  self->state.operator_id = operator_id;
  self->state.is_hidden = is_hidden;
  self->state.node = node;
  if (auto ok = self->state.start(); not ok) {
    self->quit(std::move(ok.error()));
    return connection_manager_actor<
      Elements>::behavior_type::make_empty_behavior();
  }
  self->set_down_handler([self](const caf::down_msg& msg) {
    self->state.handle_down_msg(msg);
  });
  return {
    [self](atom::read, boost::asio::ip::tcp::socket::native_handle_type handle)
      -> caf::result<chunk_ptr> {
      return self->state.read_from_connection(handle);
    },
    [self](atom::write, Elements& elements) -> caf::result<void> {
      return self->state.write_elements(std::move(elements));
    },
    [self](atom::read) -> caf::result<Elements> {
      return self->state.read_elements();
    },
    [self](uint64_t op_index, uint64_t metric_index,
           type& schema) -> caf::result<void> {
      auto& id = self->state.metrics_id_map[op_index][metric_index];
      if (id == 0) {
        id = self->state.next_metrics_id++;
      }
      return self->delegate(self->state.metrics_receiver,
                            self->state.operator_id, id, std::move(schema));
    },
    [self](uint64_t op_index, uint64_t metric_index,
           record& metric) -> caf::result<void> {
      const auto& id = self->state.metrics_id_map[op_index][metric_index];
      return self->delegate(self->state.metrics_receiver,
                            self->state.operator_id, id, std::move(metric));
    },
    [](const operator_metric& op_metric) -> caf::result<void> {
      // We have no mechanism for forwarding operator metrics. That's a bit
      // annoying, but there also really isn't a good solution to this.
      TENZIR_UNUSED(op_metric);
      return {};
    },
    [self](diagnostic& diagnostic) -> caf::result<void> {
      TENZIR_ASSERT(diagnostic.severity != severity::error);
      // TODO: The diagnostics and metrics come from the execution nodes
      // directly, so there's no way to enrich them with a native handle here.
      self->state.diagnostics.emit(std::move(diagnostic));
      return {};
    },
  };
}

// -- load_tcp operator --------------------------------------------------------

template <class Elements>
class load_tcp_operator final
  : public crtp_operator<load_tcp_operator<Elements>> {
public:
  load_tcp_operator() = default;

  load_tcp_operator(load_tcp_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<Elements> {
    const auto connection_manager_actor
      = scope_linked{ctrl.self().spawn<caf::linked>(
        make_connection_manager<Elements>, args_, ctrl.shared_diagnostics(),
        ctrl.metrics_receiver(), ctrl.operator_index(), ctrl.is_hidden(),
        ctrl.node())};
    while (true) {
      auto result = Elements{};
      ctrl.set_waiting(true);
      ctrl.self()
        .request(connection_manager_actor.get(), caf::infinite, atom::read_v)
        .then(
          [&](Elements& elements) {
            ctrl.set_waiting(false);
            result = std::move(elements);
          },
          [&](const caf::error& err) {
            diagnostic::error(err).emit(ctrl.diagnostics());
          });
      co_yield {};
      co_yield std::move(result);
    }
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto name() const -> std::string override {
    return fmt::format("internal-load-tcp-{}", operator_type_name<Elements>());
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    if (not args_.pipeline) {
      return {filter, order, this->copy()};
    }
    auto result = args_.pipeline->inner.optimize(filter, order);
    if (not result.replacement) {
      return result;
    }
    auto args = args_;
    args.pipeline->inner = pipeline{};
    args.pipeline->inner.append(std::move(result.replacement));
    result.replacement
      = std::make_unique<load_tcp_operator<Elements>>(std::move(args));
    return result;
  }

  friend auto inspect(auto& f, load_tcp_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  load_tcp_args args_ = {};
};

// -- plugins ------------------------------------------------------------------

class load_tcp_plugin final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "load_tcp";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto endpoint = located<std::string>{};
    auto parallel = std::optional<located<uint64_t>>{};
    auto tls = std::optional<located<bool>>{};
    auto args = load_tcp_args{};
    auto parser = argument_parser2::operator_("load_tcp");
    parser.add(endpoint, "<endpoint>");
    parser.add("connect", args.connect);
    parser.add("parallel", parallel);
    parser.add("tls", tls);
    parser.add("certfile", args.certfile);
    parser.add("keyfile", args.keyfile);
    parser.add(args.pipeline, "{ ... }");
    parser.parse(inv, ctx).ignore();
    auto failed = false;
    if (endpoint.inner.starts_with("tcp://")) {
      endpoint.inner = endpoint.inner.substr(6);
      endpoint.source.begin += 6;
    }
    if (const auto splits = detail::split(endpoint.inner, ":", 1);
        splits.size() != 2) {
      diagnostic::error("malformed endpoint")
        .primary(endpoint.source)
        .hint("syntax: [tcp://]<hostname>:<port>")
        .usage(parser.usage())
        .docs(parser.docs())
        .emit(ctx);
      failed = true;
    } else {
      args.endpoint.inner.hostname = std::string{splits[0]};
      args.endpoint.inner.port = std::string{splits[1]};
      args.endpoint.source = endpoint.source;
    }
    if (parallel) {
      args.parallel = *parallel;
      if (args.parallel.inner == 0
          or args.parallel.inner > std::thread::hardware_concurrency()) {
        diagnostic::error("`parallel` must be between 1 and {}",
                          std::thread::hardware_concurrency())
          .primary(parallel->source)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx);
        failed = true;
      }
      if (args.connect and args.parallel.inner != 1) {
        diagnostic::warning("`parallel` is ignored when `connect` is set")
          .primary(*args.connect)
          .primary(parallel->source)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx);
      }
    } else {
      args.parallel = {1, location::unknown};
    }
    if (tls and not tls->inner) {
      if (args.certfile) {
        diagnostic::error("conflicting option: `certfile` requires `tls`")
          .primary(tls->source)
          .primary(args.certfile->source)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx);
        failed = true;
      }
      if (args.keyfile) {
        diagnostic::error("conflicting option: `keyfile` requires `tls`")
          .primary(tls->source)
          .primary(args.keyfile->source)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx);
        failed = true;
      }
    }
    if (tls) {
      args.tls.emplace(tls->source);
    } else if (args.certfile or args.keyfile) {
      args.tls.emplace(location::unknown);
    }
    if (not args.pipeline) {
      // If the user does not provide a pipeline, we fall back to just an empty
      // pipeline, i.e., pass the bytes for all connections through.
      args.pipeline.emplace(pipeline{}, location::unknown);
    }
    const auto output_type = args.pipeline->inner.infer_type(tag_v<chunk_ptr>);
    if (not output_type) {
      diagnostic::error(output_type.error())
        .note("failed to infer output type of nested pipeline")
        .primary(args.pipeline->source)
        .usage(parser.usage())
        .docs(parser.docs())
        .emit(ctx);
      failed = true;
    }
    if (failed) {
      return failure::promise();
    }
    return output_type->match(
      [&](tag<void>) -> failure_or<operator_ptr> {
        diagnostic::error("nested pipeline must return bytes or events")
          .primary(args.pipeline->source)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx);
        return failure::promise();
      },
      [&](tag<chunk_ptr>) -> failure_or<operator_ptr> {
        return std::make_unique<load_tcp_operator<chunk_ptr>>(std::move(args));
      },
      [&](tag<table_slice>) -> failure_or<operator_ptr> {
        return std::make_unique<load_tcp_operator<table_slice>>(
          std::move(args));
      });
  }
};

using load_tcp_bytes_plugin
  = operator_inspection_plugin<load_tcp_operator<chunk_ptr>>;
using load_tcp_events_plugin
  = operator_inspection_plugin<load_tcp_operator<table_slice>>;
using load_tcp_source_bytes_plugin
  = operator_inspection_plugin<load_tcp_source_operator>;
using load_tcp_sink_bytes_plugin
  = operator_inspection_plugin<load_tcp_sink_operator<chunk_ptr>>;
using load_tcp_sink_events_plugin
  = operator_inspection_plugin<load_tcp_sink_operator<table_slice>>;

} // namespace

} // namespace tenzir::plugins::load_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_tcp::load_tcp_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_tcp::load_tcp_bytes_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_tcp::load_tcp_events_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_tcp::load_tcp_source_bytes_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_tcp::load_tcp_sink_bytes_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_tcp::load_tcp_sink_events_plugin)
