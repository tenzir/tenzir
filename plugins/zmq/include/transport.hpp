//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/chunk.hpp>
#include <tenzir/error.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <zmq.hpp>

namespace tenzir::plugins::zmq::transport {

enum class SocketRole {
  publisher,
  subscriber,
};

enum class ConnectionMode {
  bind,
  connect,
};

inline constexpr auto default_endpoint = std::string_view{"tcp://127.0.0.1:5555"};

auto normalize_endpoint(std::string endpoint) -> std::string;

auto is_tcp_endpoint(std::string_view endpoint) -> bool;

auto render_monitor_event(uint16_t event) -> std::string_view;

auto strip_prefix(chunk_ptr message, std::string_view prefix)
  -> caf::expected<chunk_ptr>;

auto prepend_prefix(chunk_ptr payload, std::string_view prefix)
  -> caf::expected<chunk_ptr>;

class Socket {
public:
  explicit Socket(SocketRole role);
  ~Socket();

  Socket(Socket const&) = delete;
  auto operator=(Socket const&) -> Socket& = delete;
  Socket(Socket&&) noexcept;
  auto operator=(Socket&&) noexcept -> Socket&;

  auto enable_peer_monitoring() -> caf::expected<void>;

  auto open(ConnectionMode mode, std::string_view endpoint,
            std::chrono::milliseconds reconnect_interval
            = std::chrono::seconds{1}) -> caf::expected<void>;

  auto set_subscription_prefix(std::string_view prefix) -> caf::expected<void>;

  auto send(const chunk_ptr& chunk,
            std::optional<std::chrono::milliseconds> timeout = {})
    -> caf::error;

  auto receive(std::optional<std::chrono::milliseconds> timeout = {})
    -> caf::expected<chunk_ptr>;

  auto poll_monitor(std::optional<std::chrono::milliseconds> timeout = {})
    -> size_t;

  auto monitored() const -> bool;

  auto num_peers() const -> size_t;

private:
  class Monitor;

  auto make_error(const ::zmq::error_t& error) const -> caf::error;

  auto make_error(int error_number) const -> caf::error;

  auto make_error() const -> caf::error;

  ::zmq::socket_t socket_;
  std::unique_ptr<Monitor> monitor_;
  size_t num_peers_ = 0;
};

} // namespace tenzir::plugins::zmq::transport
