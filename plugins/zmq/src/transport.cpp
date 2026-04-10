//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "transport.hpp"

#include <tenzir/detail/narrow.hpp>
#include <tenzir/uuid.hpp>

#include <array>
#include <cstring>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq::transport {

namespace {

auto global_context() -> ::zmq::context_t& {
  static auto ctx = ::zmq::context_t{};
  return ctx;
}

auto poll(::zmq::socket_t& socket, short flags,
          std::optional<std::chrono::milliseconds> timeout = {}) -> bool {
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

struct MonitorEvent {
  uint16_t event = 0;
  int32_t value = 0;
  std::string address;
};

} // namespace

class Socket::Monitor {
public:
  Monitor() = default;

  explicit Monitor(::zmq::socket_t& socket) {
    auto endpoint = fmt::format("inproc://monitor-{}", uuid::random());
    int rc = zmq_socket_monitor(socket.handle(), endpoint.c_str(), ZMQ_EVENT_ALL);
    if (rc != 0) {
      throw ::zmq::error_t{};
    }
    monitor_socket_ = ::zmq::socket_t{global_context(), ::zmq::socket_type::pair};
    monitor_socket_.connect(endpoint.c_str());
  }

  auto events(std::optional<std::chrono::milliseconds> timeout = {})
    -> std::vector<MonitorEvent> {
    auto result = std::vector<MonitorEvent>{};
    if (not poll(monitor_socket_, ZMQ_POLLIN, timeout)) {
      return result;
    }
    do {
      auto event = MonitorEvent{};
      auto event_msg = ::zmq::message_t{};
      auto flags = ::zmq::recv_flags::none;
      auto bytes = monitor_socket_.recv(event_msg, flags);
      TENZIR_ASSERT(bytes);
      auto* ptr = event_msg.data<char>();
      std::memcpy(&event.event, ptr, sizeof(uint16_t));
      std::memcpy(&event.value, ptr + sizeof(uint16_t), sizeof(int32_t));
      auto addr_msg = ::zmq::message_t{};
      bytes = monitor_socket_.recv(addr_msg, flags);
      TENZIR_ASSERT(bytes);
      event.address = addr_msg.to_string();
      result.push_back(std::move(event));
    } while (poll(monitor_socket_, ZMQ_POLLIN, 0ms));
    return result;
  }

private:
  ::zmq::socket_t monitor_socket_;
};

auto normalize_endpoint(std::string endpoint) -> std::string {
  if (endpoint.find("://") == std::string::npos) {
    endpoint = fmt::format("tcp://{}", endpoint);
  }
  return endpoint;
}

auto is_tcp_endpoint(std::string_view endpoint) -> bool {
  return endpoint.starts_with("tcp://");
}

auto render_monitor_event(uint16_t event) -> std::string_view {
  switch (event) {
    default:
      return "unknown ZMQ_EVENT";
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

auto strip_prefix(chunk_ptr message, std::string_view prefix)
  -> caf::expected<chunk_ptr> {
  if (not message or prefix.empty()) {
    return message;
  }
  auto view = std::string_view{
    reinterpret_cast<char const*>(message->data()),
    message->size(),
  };
  if (not view.starts_with(prefix)) {
    return caf::make_error(ec::invalid_argument,
                           fmt::format("message does not start with prefix `{}`",
                                       prefix));
  }
  auto offset = prefix.size();
  return chunk::make(message->data() + offset, message->size() - offset,
                     [message = std::move(message)]() noexcept {
                       static_cast<void>(message);
                     });
}

auto prepend_prefix(chunk_ptr payload, std::string_view prefix)
  -> caf::expected<chunk_ptr> {
  if (not payload or prefix.empty()) {
    return payload;
  }
  auto buffer = std::string{};
  buffer.reserve(prefix.size() + payload->size());
  buffer.append(prefix);
  buffer.append(reinterpret_cast<char const*>(payload->data()), payload->size());
  return chunk::make(std::move(buffer));
}

Socket::Socket(SocketRole role) : role_{role} {
}

Socket::~Socket() = default;

Socket::Socket(Socket&&) noexcept = default;

auto Socket::operator=(Socket&&) noexcept -> Socket& = default;

auto Socket::enable_peer_monitoring() -> caf::expected<void> {
  try {
    if (auto err = ensure_socket(); not err) {
      return err;
    }
    monitor_ = std::make_unique<Monitor>(*socket_);
    return {};
  } catch (const ::zmq::error_t& e) {
    return make_error(e);
  }
}

auto Socket::open(ConnectionMode mode, std::string_view endpoint,
                  std::chrono::milliseconds reconnect_interval)
  -> caf::expected<void> {
  try {
    if (auto err = ensure_socket(); not err) {
      return err;
    }
    auto ms = detail::narrow_cast<int>(reconnect_interval.count());
    auto endpoint_string = std::string{endpoint};
    socket_->set(::zmq::sockopt::reconnect_ivl, ms);
    if (mode == ConnectionMode::bind) {
      socket_->bind(endpoint_string.c_str());
    } else {
      socket_->connect(endpoint_string.c_str());
    }
    return {};
  } catch (const ::zmq::error_t& e) {
    return make_error(e);
  }
}

auto Socket::set_subscription_prefix(std::string_view prefix)
  -> caf::expected<void> {
  try {
    if (auto err = ensure_socket(); not err) {
      return err;
    }
    socket_->set(::zmq::sockopt::subscribe, prefix);
    return {};
  } catch (const ::zmq::error_t& e) {
    return make_error(e);
  }
}

auto Socket::send(const chunk_ptr& chunk,
                  std::optional<std::chrono::milliseconds> timeout)
  -> caf::error {
  try {
    TENZIR_ASSERT(socket_);
    if (not poll(*socket_, ZMQ_POLLOUT, timeout)) {
      return caf::make_error(ec::timeout, "timed out while polling socket");
    }
    auto message = ::zmq::message_t{chunk->begin(), chunk->end()};
    auto bytes = socket_->send(message, ::zmq::send_flags::none);
    TENZIR_ASSERT(bytes);
    return {};
  } catch (const ::zmq::error_t& e) {
    return make_error(e);
  }
}

auto Socket::receive(std::optional<std::chrono::milliseconds> timeout)
  -> caf::expected<chunk_ptr> {
  try {
    TENZIR_ASSERT(socket_);
    if (not poll(*socket_, ZMQ_POLLIN, timeout)) {
      return caf::make_error(ec::timeout, "timed out while polling socket");
    }
    auto message = std::make_shared<::zmq::message_t>();
    auto bytes = socket_->recv(*message, ::zmq::recv_flags::none);
    TENZIR_ASSERT(bytes);
    auto* data = message->data();
    auto size = message->size();
    return chunk::make(data, size, [message = std::move(message)]() noexcept {
      static_cast<void>(message);
    });
  } catch (const ::zmq::error_t& e) {
    return make_error(e);
  }
}

auto Socket::poll_monitor(std::optional<std::chrono::milliseconds> timeout)
  -> size_t {
  if (not monitor_) {
    return 0;
  }
  auto num_events = size_t{0};
  for (auto& event : monitor_->events(timeout)) {
    ++num_events;
    switch (event.event) {
      default:
        break;
      case ZMQ_EVENT_HANDSHAKE_SUCCEEDED:
        ++num_peers_;
        break;
      case ZMQ_EVENT_DISCONNECTED:
        if (num_peers_ > 0) {
          --num_peers_;
        }
        break;
    }
  }
  return num_events;
}

auto Socket::monitored() const -> bool {
  return monitor_ != nullptr;
}

auto Socket::num_peers() const -> size_t {
  return num_peers_;
}

auto Socket::last_error() const -> const std::string& {
  return last_error_;
}

auto Socket::ensure_socket() -> caf::expected<void> {
  if (socket_) {
    return {};
  }
  try {
    socket_.emplace(global_context(), role_ == SocketRole::publisher
                                      ? ::zmq::socket_type::pub
                                      : ::zmq::socket_type::sub);
    socket_->set(::zmq::sockopt::linger, 0);
    return {};
  } catch (const ::zmq::error_t& e) {
    return make_error(e);
  }
}

auto Socket::make_error(const ::zmq::error_t& error) const -> caf::error {
  last_error_ = error.what();
  return caf::make_error(ec::unspecified,
                         fmt::format("ZeroMQ: {}", error.what()));
}

auto Socket::make_error(int error_number) const -> caf::error {
  return make_error(::zmq::error_t{error_number});
}

auto Socket::make_error() const -> caf::error {
  return make_error(zmq_errno());
}

} // namespace tenzir::plugins::zmq::transport
