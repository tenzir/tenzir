//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <zmq.hpp>

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

/// The default ZeroMQ socket endpoint.
constexpr auto default_endpoint = "tcp://127.0.0.1:5555";

/// This is the 0mq context singleton. There exists exactly one context per
/// process so that inproc sockets can be used across pipelines within the same
/// node. The plugin initializes and deinitializes the singleton. Since
/// accessing a 0mq context instance is thread-safe, we can share it globally.
std::shared_ptr<::zmq::context_t> context;

struct saver_args {
  std::optional<located<std::string>> endpoint;
  std::optional<location> connect;
  std::optional<location> listen;
  std::optional<location> monitor;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("endpoint", x.endpoint), f.field("listen", x.listen),
              f.field("connect", x.connect), f.field("monitor", x.monitor));
  }
};

struct loader_args {
  std::optional<located<std::string>> endpoint;
  std::optional<located<std::string>> filter;
  std::optional<location> connect;
  std::optional<location> listen;
  std::optional<location> monitor;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("endpoint", x.endpoint), f.field("filter", x.filter),
              f.field("listen", x.listen), f.field("connect", x.connect),
              f.field("monitor", x.monitor));
  }
};

auto render_event(uint16_t event) -> std::string_view {
  switch (event) {
    default:
      return "unknown ZMQ EVENT";
    case ZMQ_EVENT_CONNECTED:
      return "ZMQ_EVENT_CONNECTED";
    case ZMQ_EVENT_CONNECT_DELAYED:
      return "ZMQ_EVENT_CONNECT_DELAYED";
    case ZMQ_EVENT_CONNECT_RETRIED:
      return "ZMQ_EVENT_CONNECT_RETRIED";
    case ZMQ_EVENT_LISTENING:
      return "ZMQ_EVENT_LISTENING";
    case ZMQ_EVENT_BIND_FAILED:
      return "ZMQ_EVENT_BIND_FAILED";
    case ZMQ_EVENT_ACCEPTED:
      return "ZMQ_EVENT_ACCEPTED";
    case ZMQ_EVENT_ACCEPT_FAILED:
      return "ZMQ_EVENT_ACCEPT_FAILED";
    case ZMQ_EVENT_CLOSED:
      return "ZMQ_EVENT_CLOSED";
    case ZMQ_EVENT_CLOSE_FAILED:
      return "ZMQ_EVENT_CLOSE_FAILED";
    case ZMQ_EVENT_DISCONNECTED:
      return "ZMQ_EVENT_DISCONNECTED";
    case ZMQ_EVENT_MONITOR_STOPPED:
      return "ZMQ_EVENT_MONITOR_STOPPED";
    case ZMQ_EVENT_HANDSHAKE_FAILED_AUTH:
      return "ZMQ_EVENT_HANDSHAKE_FAILED_AUTH";
    case ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL:
      return "ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL";
    case ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL:
      return "ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL";
    case ZMQ_EVENT_HANDSHAKE_SUCCEEDED:
      return "ZMQ_EVENT_HANDSHAKE_SUCCEEDED";
  }
}

/// A 0mq socket that comes with a built-in monitoring socket.
class connection {
  /// Data from one monitoring cycle, i.e., one event and one address message.
  struct monitor_event {
    uint16_t event{0};
    int32_t value{0};
    std::string address{};
  };

  // An alternative to zmq::monitor_t where we don't have to override *every*
  // virtual callback function to get a feed of events.
  class monitor {
  public:
    monitor() = default;

    /// Constructs a monitor for a given socket.
    monitor(::zmq::context_t& ctx, ::zmq::socket_t& socket) {
      auto endpoint = fmt::format("inproc://monitor-{}", uuid::random());
      TENZIR_DEBUG("creating monitor on {}", endpoint);
      int rc
        = zmq_socket_monitor(socket.handle(), endpoint.c_str(), ZMQ_EVENT_ALL);
      if (rc != 0) {
        throw ::zmq::error_t{}; // fit into the cppzmq error paradigm
      }
      monitor_socket_ = ::zmq::socket_t{ctx, ::zmq::socket_type::pair};
      monitor_socket_.connect(endpoint);
    }

    /// Blocks and retrieves all available monitoring events.
    /// @param tiemout An upper bound on the block time of the monitoring
    /// socket.
    /// @returns all available monitoring events or an empty generator if non
    /// are available within the polling window.
    auto events(std::optional<std::chrono::milliseconds> timeout = {})
      -> generator<monitor_event> {
      auto ready = connection::poll(monitor_socket_, ZMQ_POLLIN, timeout);
      if (not ready) {
        co_return;
      }
      do {
        monitor_event result;
        auto event_msg = ::zmq::message_t{};
        auto flags = ::zmq::recv_flags::none;
        auto bytes = monitor_socket_.recv(event_msg, flags);
        TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
        const auto* ptr = event_msg.data<char>();
        std::memcpy(&result.event, ptr, sizeof(uint16_t));
        std::memcpy(&result.value, ptr + sizeof(uint16_t), sizeof(int32_t));
        auto addr_msg = ::zmq::message_t{};
        bytes = monitor_socket_.recv(addr_msg, flags);
        TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
        result.address = addr_msg.to_string();
        co_yield result;
      } while (connection::poll(monitor_socket_, ZMQ_POLLIN, 0ms));
    }

  private:
    monitor(::zmq::socket_t&& socket) : monitor_socket_{std::move(socket)} {
    }

    ::zmq::socket_t monitor_socket_;
  };

public:
  static auto
  make_source(std::shared_ptr<::zmq::context_t> ctx, const loader_args& args)
    -> caf::expected<connection> {
    try {
      auto result = connection{std::move(ctx), ::zmq::socket_type::sub};
      const auto& endpoint = args.endpoint->inner;
      if (args.monitor) {
        TENZIR_ASSERT(endpoint.starts_with("tcp://"));
        result.monitor();
      }
      if (args.listen) {
        result.listen(endpoint);
      } else {
        result.connect(endpoint);
      }
      auto filter = args.filter ? args.filter->inner : "";
      result.socket_.set(::zmq::sockopt::subscribe, filter);
      return result;
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  static auto make_sink(std::shared_ptr<::zmq::context_t> ctx,
                        const saver_args& args) -> caf::expected<connection> {
    try {
      auto result = connection{std::move(ctx), ::zmq::socket_type::pub};
      const auto& endpoint = args.endpoint->inner;
      if (args.monitor) {
        TENZIR_ASSERT(endpoint.starts_with("tcp://"));
        result.monitor();
      }
      if (args.connect) {
        result.connect(endpoint);
      } else {
        result.listen(endpoint);
      }
      return result;
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  auto send(chunk_ptr chunk, std::optional<std::chrono::milliseconds> timeout
                             = {}) -> caf::error {
    try {
      TENZIR_TRACE("waiting until socket is ready to send");
      if (not poll(socket_, ZMQ_POLLOUT, timeout)) {
        return caf::make_error(ec::timeout, "timed out while polling socket");
      }
      auto message = ::zmq::message_t{*chunk};
      auto flags = ::zmq::send_flags::none;
      auto bytes = socket_.send(message, flags);
      TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
      TENZIR_TRACE("sent message with {} bytes", *bytes);
      return {};
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  auto receive(std::optional<std::chrono::milliseconds> timeout = {})
    -> caf::expected<chunk_ptr> {
    try {
      TENZIR_TRACE("waiting until socket is ready to receive");
      if (not poll(socket_, ZMQ_POLLIN, timeout)) {
        return caf::make_error(ec::timeout, "timed out while polling socket");
      }
      auto message = std::make_shared<::zmq::message_t>();
      auto flags = ::zmq::recv_flags::none;
      auto bytes = socket_.recv(*message, flags);
      TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
      TENZIR_TRACE("got 0mq message with {} bytes", *bytes);
      const auto* data = message->data();
      auto size = message->size();
      auto deleter = [msg = std::move(message)]() noexcept {};
      return chunk::make(data, size, deleter);
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  /// Checks whether the socket is equipped with a monitor
  auto monitored() const -> bool {
    return monitor_ != std::nullopt;
  }

  /// Returns the number of processed monitoring events.
  auto poll_monitor(std::optional<std::chrono::milliseconds> timeout = {})
    -> size_t {
    auto num_events = size_t{0};
    for (auto&& event : monitor_->events(timeout)) {
      ++num_events;
      TENZIR_DEBUG("got monitor event: {}", render_event(event.event));
      switch (event.event) {
        default:
          break;
        case ZMQ_EVENT_HANDSHAKE_SUCCEEDED:
          ++num_peers_;
          break;
        case ZMQ_EVENT_DISCONNECTED:
          if (num_peers_ == 0) {
            TENZIR_WARN("logic error: disconnect while no one is connected");
          } else {
            --num_peers_;
          }
          break;
      }
    }
    return num_events;
  }

  auto num_peers() const -> size_t {
    return num_peers_;
  }

private:
  static auto make_error(const ::zmq::error_t& error) -> caf::error {
    return caf::make_error(ec::unspecified,
                           fmt::format("ZeroMQ: {}", error.what()));
  }

  static auto make_error(int error_number) -> caf::error {
    return make_error(::zmq::error_t{error_number});
  }

  static auto make_error() -> caf::error {
    return make_error(zmq_errno());
  }

  static auto poll(::zmq::socket_t& socket, short flags,
                   std::optional<std::chrono::milliseconds> timeout = {})
    -> bool {
    auto items = std::array<::zmq::pollitem_t, 1>{
      {{socket.handle(), 0, flags, 0}},
    };
    auto infinite = std::chrono::milliseconds(-1);
    auto ms = timeout ? *timeout : infinite;
    auto num_events_signaled = ::zmq::poll(items.data(), items.size(), ms);
    if (num_events_signaled == 0) {
      return false;
    }
    TENZIR_ASSERT(num_events_signaled > 0);
    TENZIR_ASSERT((items[0].revents & flags) != 0);
    return true;
  }

  connection() = default;

  connection(std::shared_ptr<::zmq::context_t> ctx,
             ::zmq::socket_type socket_type)
    : ctx_{std::move(ctx)}, socket_{*ctx_, socket_type} {
    // The linger period determines how long pending messages which have yet
    // to be sent to a peer shall linger in memory after a socket is closed
    // with zmq_close(3), and further affects the termination of the socket's
    // context with zmq_term(3).
    //
    // The value of 0 specifies no linger period. Pending messages shall be
    // discarded immediately when the socket is closed with zmq_close().
    socket_.set(::zmq::sockopt::linger, 0);
  }

  /// Sets up a monitoring socket for this connection.
  auto monitor() -> void {
    monitor_ = {*ctx_, socket_};
  }

  /// Starts listening on the provided endpoint.
  auto listen(const std::string& endpoint,
              std::chrono::milliseconds reconnect_interval = 1s) -> void {
    TENZIR_VERBOSE("listening to endpoint {}", endpoint);
    auto ms = detail::narrow_cast<int>(reconnect_interval.count());
    socket_.set(::zmq::sockopt::reconnect_ivl, ms); // for TCP only, not inproc
    socket_.bind(endpoint);
  }

  /// Connects to the provided endpoint.
  auto connect(const std::string& endpoint,
               std::chrono::milliseconds reconnect_interval = 1s) -> void {
    TENZIR_VERBOSE("connecting to endpoint {}", endpoint);
    auto ms = detail::narrow_cast<int>(reconnect_interval.count());
    socket_.set(::zmq::sockopt::reconnect_ivl, ms); // for TCP only, not inproc
    socket_.connect(endpoint);
  }

  std::shared_ptr<::zmq::context_t> ctx_;
  ::zmq::socket_t socket_;
  std::optional<class monitor> monitor_;
  size_t num_peers_{0};
};

class zmq_loader final : public plugin_loader {
public:
  zmq_loader() = default;

  zmq_loader(loader_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto conn = connection::make_source(context, args_);
    if (not conn) {
      TENZIR_ERROR(conn.error());
      return std::nullopt;
    }
    auto make = [](operator_control_plane& ctrl,
                   connection conn) mutable -> generator<chunk_ptr> {
      while (true) {
        if (conn.monitored()) {
          // Poll in larger strides if we have no peers. Once we have at least
          // one peer, there is no need to wait on monitoring events.
          auto timeout = conn.num_peers() == 0 ? 500ms : 0ms;
          conn.poll_monitor(timeout);
          if (conn.num_peers() == 0) {
            co_yield {};
            continue;
          }
        }
        if (auto message = conn.receive(250ms)) {
          co_yield *message;
        } else if (message == ec::timeout) {
          co_yield {};
        } else {
          diagnostic::error(message.error()).emit(ctrl.diagnostics());
          break;
        }
      }
    };
    return make(ctrl, std::move(*conn));
  }

  auto name() const -> std::string override {
    return "zmq";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  auto internal() const -> bool override {
    return args_.endpoint and args_.endpoint->inner.starts_with("inproc://");
  }

  friend auto inspect(auto& f, zmq_loader& x) -> bool {
    return f.object(x)
      .pretty_name("zmq_loader")
      .fields(f.field("args", x.args_));
  }

private:
  loader_args args_;
};

class zmq_saver final : public plugin_saver {
public:
  zmq_saver() = default;

  zmq_saver(saver_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto conn = connection::make_sink(context, args_);
    if (not conn) {
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to setup ZeroMQ: {}",
                                         conn.error()));
    }
    return [&ctrl, conn = std::make_shared<connection>(std::move(*conn))](
             chunk_ptr chunk) mutable {
      if (not chunk || chunk->size() == 0) {
        return;
      }
      if (conn->monitored()) {
        // Block until we have at least one peer, or fast-track with a zero
        // timeout when in steady state.
        do {
          auto timeout = conn->num_peers() == 0 ? 500ms : 0ms;
          conn->poll_monitor(timeout);
        } while (conn->num_peers() == 0);
      }
      if (auto error = conn->send(chunk)) {
        diagnostic::error(error).emit(ctrl.diagnostics());
      }
    };
  }

  auto name() const -> std::string override {
    return "zmq";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto internal() const -> bool override {
    return args_.endpoint and args_.endpoint->inner.starts_with("inproc://");
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, zmq_saver& x) -> bool {
    return f.object(x).pretty_name("zmq_saver").fields(f.field("args", x.args_));
  }

private:
  saver_args args_;
};

class plugin final : public virtual loader_plugin<zmq_loader>,
                     public virtual saver_plugin<zmq_saver> {
public:
  ~plugin() noexcept override {
    // Destroy the singleton.
    context.reset();
  }

  auto initialize(const record&, const record&) -> caf::error override {
    // Create the singleton.
    context = std::make_shared<::zmq::context_t>();
    return {};
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = loader_args{};
    parser.add(args.endpoint, "<endpoint>");
    parser.add("-f,--filter", args.filter, "<prefix>");
    parser.add("-l,--listen", args.listen);
    parser.add("-c,--connect", args.connect);
    parser.add("-m,--monitor", args.monitor);
    parser.parse(p);
    if (args.listen && args.connect) {
      diagnostic::error("both --listen and --connect provided")
        .primary(*args.listen)
        .primary(*args.connect)
        .hint("--listen and --connect are mutually exclusive")
        .throw_();
    }
    if (not args.endpoint) {
      args.endpoint = located<std::string>{default_endpoint, location::unknown};
    } else if (args.endpoint->inner.find("://") == std::string::npos) {
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    }
    if (not args.endpoint->inner.starts_with("tcp://") and args.monitor) {
      diagnostic::error("--monitor with incompatible scheme")
        .primary(*args.monitor)
        .note("--monitor requires a TCP endpoint")
        .hint("switch to tcp://host:port or remove --monitor")
        .throw_();
    }
    return std::make_unique<zmq_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = saver_args{};
    parser.add(args.endpoint, "<endpoint>");
    parser.add("-l,--listen", args.listen);
    parser.add("-c,--connect", args.connect);
    parser.add("-m,--monitor", args.monitor);
    parser.parse(p);
    if (args.listen && args.connect) {
      diagnostic::error("both --listen and --connect provided")
        .primary(*args.listen)
        .primary(*args.connect)
        .hint("--listen and --connect are mutually exclusive")
        .throw_();
    }
    if (not args.endpoint) {
      args.endpoint = located<std::string>{default_endpoint, location::unknown};
    } else if (args.endpoint->inner.find("://") == std::string::npos) {
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    }
    if (not args.endpoint->inner.starts_with("tcp://") and args.monitor) {
      diagnostic::error("--monitor with incompatible scheme")
        .primary(*args.monitor)
        .note("--monitor requires a TCP endpoint")
        .hint("switch to tcp://host:port or remove --monitor")
        .throw_();
    }
    return std::make_unique<zmq_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "zmq";
  }

  auto supported_uri_schemes() const -> std::vector<std::string> override {
    return {"zmq", "inproc"};
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::plugin)
