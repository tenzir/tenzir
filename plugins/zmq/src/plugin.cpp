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

namespace tenzir::plugins::zmq {

namespace {

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

struct connector_args {
  std::string endpoint = "tcp://127.0.0.1:5555";
  std::optional<location> connect;
  std::optional<location> bind;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("connector_args")
      .fields(f.field("endpoint", x.endpoint), f.field("bind", x.bind),
              f.field("connect", x.connect));
  }
};

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

    /// Waits for one monitoring event consisting of two messages.
    auto get() -> monitor_event {
      auto poll_result = engine::poll(monitor_socket_, ZMQ_POLLIN);
      TENZIR_ASSERT(poll_result == true); // we wait indefinitely.
      monitor_event result;
      auto event_msg = ::zmq::message_t{};
      auto flags = ::zmq::recv_flags::none;
      auto bytes = monitor_socket_.recv(event_msg, flags);
      TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
      const auto* ptr = event_msg.data<char>();
      std::memcpy(&result.event, ptr, sizeof(uint16_t));
      std::memcpy(&result.value, ptr + sizeof(uint16_t), sizeof(int32_t));
      TENZIR_DEBUG("got monitor event message: <{}, {}>",
                   render_event(result.event), result.value);
      auto addr_msg = ::zmq::message_t{};
      bytes = monitor_socket_.recv(addr_msg, flags);
      TENZIR_ASSERT(bytes); // only nullopt in non-blocking mode.
      result.address = addr_msg.to_string();
      TENZIR_DEBUG("got monitor address message: {}", result.address);
      return result;
    }

  private:
    monitor(::zmq::socket_t&& socket) : monitor_socket_{std::move(socket)} {
    }

    ::zmq::socket_t monitor_socket_;
  };

public:
  static auto make_source(const connector_args& args) -> caf::expected<engine> {
    try {
      auto result = engine{::zmq::socket_type::sub};
      if (args.bind)
        result.bind(args.endpoint);
      else
        result.connect(args.endpoint);
      const auto* everything = "";
      result.socket_.set(::zmq::sockopt::subscribe, everything);
      return result;
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  static auto make_sink(const connector_args& args) -> caf::expected<engine> {
    try {
      auto result = engine{::zmq::socket_type::pub};
      if (args.connect)
        result.connect(args.endpoint);
      else
        result.bind(args.endpoint);
      return result;
    } catch (const ::zmq::error_t& e) {
      return make_error(e);
    }
  }

  auto send(chunk_ptr chunk) -> caf::error {
    try {
      TENZIR_DEBUG("waiting until socket is ready to send");
      if (not poll(socket_, ZMQ_POLLOUT))
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

  auto receive() -> caf::expected<chunk_ptr> {
    try {
      TENZIR_DEBUG("waiting until socket is ready to receive");
      if (not poll(socket_, ZMQ_POLLIN))
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

private:
  static auto make_error(const ::zmq::error_t& error) -> caf::error {
    return caf::make_error(ec::unspecified,
                           fmt::format("ZeroMQ: {} ({})", error.what(),
                                       error.num()));
  }

  static auto poll(::zmq::socket_t& socket, short flags,
                   std::optional<std::chrono::milliseconds> timeout = {})
    -> bool {
    auto items = std::array<::zmq::pollitem_t, 1>{
      {{socket.handle(), 0, flags, 0}},
    };
    auto ms = timeout ? *timeout : std::chrono::milliseconds(-1);
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

  auto bind(const std::string& endpoint) -> bool {
    TENZIR_VERBOSE("binding to endpoint {}", endpoint);
    socket_.bind(endpoint);
    while (true) {
      auto event = monitor_.get();
      TENZIR_DEBUG("got monitor event: {}", render_event(event.event));
      switch (event.event) {
        default:
          break;
        case ZMQ_EVENT_ACCEPTED:
          return true;
        case ZMQ_EVENT_ACCEPT_FAILED:
        case ZMQ_EVENT_HANDSHAKE_FAILED_AUTH:
        case ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL:
        case ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL:
          return false;
      }
    }
    return true;
  }

  auto connect(const std::string& endpoint) -> bool {
    TENZIR_VERBOSE("binding to endpoint {}", endpoint);
    socket_.set(::zmq::sockopt::reconnect_ivl, 250);
    socket_.connect(endpoint);
    // The sequence of events we receive when 0mq polls during connecting is as
    // follows:
    //     1. ZMQ_EVENT_CONNECT_DELAYED
    //     2. ZMQ_EVENT_CLOSED
    //     3. ZMQ_EVENT_CONNECT_RETRIED
    while (true) {
      auto event = monitor_.get();
      TENZIR_DEBUG("got monitor event: {}", render_event(event.event));
      switch (event.event) {
        default:
          break;
        case ZMQ_EVENT_CONNECTED:
          return true;
        case ZMQ_EVENT_HANDSHAKE_FAILED_AUTH:
        case ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL:
        case ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL:
          return false;
      }
    }
    return true;
  }

  ::zmq::context_t ctx_;
  ::zmq::socket_t socket_;
  monitor monitor_;
};

class zmq_loader final : public plugin_loader {
public:
  zmq_loader() = default;

  zmq_loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto engine = engine::make_source(args_);
    if (not engine) {
      TENZIR_ERROR(engine.error());
      return std::nullopt;
    }
    auto make = [&ctrl](class engine engine) mutable -> generator<chunk_ptr> {
      if (auto message = engine.receive())
        co_yield *message;
      else
        ctrl.abort(message.error());
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
    if (args_.bind)
      result += " --bind";
    if (args_.connect)
      result += " --connect";
    return result;
  }

private:
  connector_args args_;
};

class zmq_saver final : public plugin_saver {
public:
  zmq_saver() = default;

  zmq_saver(connector_args args) : args_{std::move(args)} {
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
  connector_args args_;
};

class plugin final : public virtual loader_plugin<zmq_loader>,
                     public virtual saver_plugin<zmq_saver> {
public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = connector_args{};
    parse(p, args);
    return std::make_unique<zmq_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = connector_args{};
    parse(p, args);
    return std::make_unique<zmq_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "zmq";
  }

private:
  static void parse(parser_interface& p, connector_args& args) {
    auto parser
      = argument_parser{"zmq", "https://docs.tenzir.com/docs/connectors/zmq"};
    auto endpoint = std::optional<located<std::string>>{};
    parser.add(endpoint, "<endpoint>");
    parser.add("-b,--bind", args.bind);
    parser.add("-c,--connect", args.connect);
    parser.add(endpoint, "<endpoint>");
    parser.parse(p);
    if (endpoint)
      args.endpoint = std::move(endpoint->inner);
    if (args.bind && args.connect)
      diagnostic::error("both --bind and --connect provided")
        .primary(*args.bind)
        .primary(*args.connect)
        .hint("--bind and --connect are mutually exclusive")
        .throw_();
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::plugin)
