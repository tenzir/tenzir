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

#include <memory>
#include <optional>
#include <string>
#include <zmq.hpp>

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

/// The default ZeroMQ socket endpoint.
constexpr auto default_endpoint = "tcp://127.0.0.1:5555";

struct saver_args {
  std::optional<located<std::string>> endpoint;
  std::optional<location> connect;
  std::optional<location> bind;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("endpoint", x.endpoint), f.field("bind", x.bind),
              f.field("connect", x.connect));
  }
};

struct loader_args {
  std::optional<located<std::string>> endpoint;
  std::optional<located<std::string>> filter;
  std::optional<location> connect;
  std::optional<location> bind;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("endpoint", x.endpoint), f.field("filter", x.filter),
              f.field("bind", x.bind), f.field("connect", x.connect));
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

/// A 0mq utility for use as source or sink operator.
class engine {
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
    monitor(::zmq::context_t& ctx, ::zmq::socket_t& socket,
            const char* addr = "monitor") {
      auto endpoint = fmt::format("inproc://{}", addr);
      TENZIR_DEBUG("creating monitor on {}", endpoint);
      int rc
        = zmq_socket_monitor(socket.handle(), endpoint.c_str(), ZMQ_EVENT_ALL);
      if (rc != 0)
        throw ::zmq::error_t{}; // fit into the cppzmq error paradigm
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
      auto ready = engine::poll(monitor_socket_, ZMQ_POLLIN, timeout);
      if (not ready)
        co_return;
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
      } while (engine::poll(monitor_socket_, ZMQ_POLLIN, 0ms));
    }

  private:
    monitor(::zmq::socket_t&& socket) : monitor_socket_{std::move(socket)} {
    }

    ::zmq::socket_t monitor_socket_;
  };

public:
  static auto make_source(const loader_args& args) -> caf::expected<engine> {
    try {
      auto result = engine{::zmq::socket_type::sub};
      auto endpoint = args.endpoint ? args.endpoint->inner : default_endpoint;
      if (args.bind)
        result.bind(endpoint);
      else
        result.connect(endpoint);
      auto filter = args.filter ? args.filter->inner : "";
      result.socket_.set(::zmq::sockopt::subscribe, filter);
      return result;
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  static auto make_sink(const saver_args& args) -> caf::expected<engine> {
    try {
      auto result = engine{::zmq::socket_type::pub};
      auto endpoint = args.endpoint ? args.endpoint->inner : default_endpoint;
      if (args.connect)
        result.connect(endpoint);
      else
        result.bind(endpoint);
      return result;
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  auto send(chunk_ptr chunk, std::optional<std::chrono::milliseconds> timeout
                             = {}) -> caf::error {
    try {
      TENZIR_DEBUG("waiting until socket is ready to send");
      if (not poll(socket_, ZMQ_POLLOUT, timeout))
        return caf::make_error(ec::timeout, "timed out while polling socket");
      auto message = ::zmq::message_t{*chunk};
      auto flags = ::zmq::send_flags::none;
      auto bytes = socket_.send(message, flags);
      TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
      TENZIR_DEBUG("sent message with {} bytes", *bytes);
      return {};
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  auto receive(std::optional<std::chrono::milliseconds> timeout = {})
    -> caf::expected<chunk_ptr> {
    try {
      TENZIR_DEBUG("waiting until socket is ready to receive");
      if (not poll(socket_, ZMQ_POLLIN, timeout))
        return caf::make_error(ec::timeout, "timed out while polling socket");
      auto message = std::make_shared<::zmq::message_t>();
      auto flags = ::zmq::recv_flags::none;
      auto bytes = socket_.recv(*message, flags);
      TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
      TENZIR_DEBUG("got 0mq message with {} bytes", *bytes);
      const auto* data = message->data();
      auto size = message->size();
      auto deleter = [msg = std::move(message)]() noexcept {};
      return chunk::make(data, size, deleter);
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  /// Returns the number of processed monitoring events.
  auto poll_monitor(std::optional<std::chrono::milliseconds> timeout = {})
    -> size_t {
    auto num_events = size_t{0};
    for (auto&& event : monitor_.events(timeout)) {
      ++num_events;
      TENZIR_DEBUG("got monitor event: {}", render_event(event.event));
      switch (event.event) {
        default:
          break;
        case ZMQ_EVENT_HANDSHAKE_SUCCEEDED:
          ++num_peers_;
          break;
        case ZMQ_EVENT_DISCONNECTED:
          if (num_peers_ == 0)
            TENZIR_WARN("logic error: disconnect while no one is connected");
          else
            --num_peers_;
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
    if (num_events_signaled == 0)
      return false;
    TENZIR_ASSERT_CHEAP(num_events_signaled > 0);
    TENZIR_ASSERT_CHEAP((items[0].revents & flags) != 0);
    return true;
  }

  engine() = default;

  explicit engine(::zmq::socket_type socket_type)
    : socket_{ctx_, socket_type}, monitor_{ctx_, socket_} {
  }

  void bind(const std::string& endpoint) {
    TENZIR_VERBOSE("binding to endpoint {}", endpoint);
    socket_.bind(endpoint);
  }

  void connect(const std::string& endpoint,
               std::chrono::milliseconds reconnect_interval = 1s) {
    TENZIR_VERBOSE("connecting to endpoint {}", endpoint);
    auto ms = detail::narrow_cast<int>(reconnect_interval.count());
    socket_.set(::zmq::sockopt::reconnect_ivl, ms);
    socket_.connect(endpoint);
  }

  ::zmq::context_t ctx_;
  ::zmq::socket_t socket_;
  monitor monitor_;
  size_t num_peers_{0};
};

class zmq_loader final : public plugin_loader {
public:
  zmq_loader() = default;

  zmq_loader(loader_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto engine = engine::make_source(args_);
    if (not engine) {
      TENZIR_ERROR(engine.error());
      return std::nullopt;
    }
    auto make = [&ctrl](class engine engine) mutable -> generator<chunk_ptr> {
      while (true) {
        // Poll in larger strides when we have no peers. If we have at least one
        // peer, there is no need to wait on the monitor.
        auto timeout = engine.num_peers() == 0 ? 500ms : 0ms;
        engine.poll_monitor(timeout);
        if (engine.num_peers() == 0) {
          co_yield {};
          continue;
        }
        if (auto message = engine.receive(250ms)) {
          co_yield *message;
        } else if (message == ec::timeout) {
          co_yield {};
        } else {
          ctrl.abort(message.error());
          break;
        }
      }
    };
    return make(std::move(*engine));
  }

  auto name() const -> std::string override {
    return "zmq";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, zmq_loader& x) -> bool {
    return f.object(x)
      .pretty_name("zmq_loader")
      .fields(f.field("args", x.args_));
  }

  auto to_string() const -> std::string override {
    auto result = name();
    result += fmt::format(" {}", args_.endpoint);
    if (args_.filter)
      result += fmt::format(" --filter {}", args_.filter->inner);
    if (args_.bind)
      result += " --bind";
    if (args_.connect)
      result += " --connect";
    return result;
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
    auto engine = engine::make_sink(args_);
    if (not engine) {
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to setup ZeroMQ: {}",
                                         engine.error()));
    }
    return [&ctrl, engine = std::make_shared<class engine>(std::move(*engine))](
             chunk_ptr chunk) mutable {
      if (not chunk || chunk->size() == 0)
        return;
      // Block until we have at least one peer, or fast-track with a zero
      // timeout when in steady state.
      do {
        auto timeout = engine->num_peers() == 0 ? 500ms : 0ms;
        engine->poll_monitor(timeout);
      } while (engine->num_peers() == 0);
      if (auto error = engine->send(chunk))
        ctrl.abort(error);
    };
  }

  auto name() const -> std::string override {
    return "zmq";
  }

  auto default_printer() const -> std::string override {
    return "json";
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
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = loader_args{};
    parser.add(args.endpoint, "<endpoint>");
    parser.add("-f,--filter", args.filter, "<prefix>");
    parser.add("-b,--bind", args.bind);
    parser.add("-c,--connect", args.connect);
    parser.parse(p);
    if (args.bind && args.connect)
      diagnostic::error("both --bind and --connect provided")
        .primary(*args.bind)
        .primary(*args.connect)
        .hint("--bind and --connect are mutually exclusive")
        .throw_();
    if (args.endpoint && args.endpoint->inner.find("://") == std::string::npos)
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    return std::make_unique<zmq_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = saver_args{};
    parser.add(args.endpoint, "<endpoint>");
    parser.add("-b,--bind", args.bind);
    parser.add("-c,--connect", args.connect);
    parser.parse(p);
    if (args.bind && args.connect)
      diagnostic::error("both --bind and --connect provided")
        .primary(*args.bind)
        .primary(*args.connect)
        .hint("--bind and --connect are mutually exclusive")
        .throw_();
    if (args.endpoint && args.endpoint->inner.find("://") == std::string::npos)
      args.endpoint->inner = fmt::format("tcp://{}", args.endpoint->inner);
    return std::make_unique<zmq_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "zmq";
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::plugin)
