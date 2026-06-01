//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/uds.hpp"

#include "tenzir/detail/scope_guard.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <cerrno>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <poll.h>
#include <unistd.h>

namespace tenzir {

namespace {

constexpr auto uds_probe_timeout = std::chrono::milliseconds{100};

class UdsAcceptCallback final
  : public folly::AsyncServerSocket::AcceptCallback {
public:
  explicit UdsAcceptCallback(folly::coro::Baton& baton,
                             Arc<folly::AsyncServerSocket> socket)
    : baton_{baton}, socket_{std::move(socket)} {
  }

  auto connectionAccepted(folly::NetworkSocket fd_network_socket,
                          folly::SocketAddress const&, AcceptInfo) noexcept
    -> void override {
    unregister();
    accept_fd = fd_network_socket.toFd();
    baton_.post();
  }

  auto acceptError(folly::exception_wrapper ex) noexcept -> void override {
    unregister();
    error = std::move(ex);
    accept_fd = -1;
    baton_.post();
  }

  auto acceptStarted() noexcept -> void override {
  }

  auto acceptStopped() noexcept -> void override {
  }

  int accept_fd = -1;
  folly::exception_wrapper error = {};
  bool registered = true;

private:
  auto unregister() noexcept -> void {
    if (not registered) {
      return;
    }
    socket_->pauseAccepting();
    try {
      socket_->removeAcceptCallback(this, nullptr);
    } catch (std::exception const&) {
      // AsyncServerSocket may remove the callback as part of completing an
      // accept. The guard still has to run on cancellation, where the stack
      // callback must not remain registered.
    }
    registered = false;
  }

  folly::coro::Baton& baton_;
  Arc<folly::AsyncServerSocket> socket_;
};

auto max_uds_path_size() -> size_t {
  return sizeof(sockaddr_un{}.sun_path) - 1;
}

auto emit_probe_error(std::string const& path, location source,
                      diagnostic_handler& dh, std::string_view reason) -> void {
  diagnostic::error("failed to probe UNIX domain socket")
    .primary(source)
    .note("path: {}", path)
    .note("reason: {}", reason)
    .emit(dh);
}

auto uds_path_has_listener(std::string const& path, location source,
                           diagnostic_handler& dh) -> failure_or<bool> {
  auto fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd == -1) {
    emit_probe_error(path, source, dh, std::strerror(errno));
    return failure::promise();
  }
  auto close_fd = detail::scope_guard{[fd]() noexcept {
    ::close(fd);
  }};
  auto flags = ::fcntl(fd, F_GETFL, 0);
  if (flags == -1 or ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    emit_probe_error(path, source, dh, std::strerror(errno));
    return failure::promise();
  }
  auto address = sockaddr_un{};
  address.sun_family = AF_UNIX;
#if defined(__APPLE__)
  address.sun_len = sizeof(sockaddr_un);
#endif
  std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address))
      == 0) {
    return true;
  }
  if (errno == ECONNREFUSED or errno == ENOENT) {
    return false;
  }
  if (errno != EINPROGRESS and errno != EAGAIN and errno != EWOULDBLOCK) {
    emit_probe_error(path, source, dh, std::strerror(errno));
    return failure::promise();
  }
  auto fds = pollfd{
    .fd = fd,
    .events = POLLOUT,
    .revents = 0,
  };
  auto timeout = static_cast<int>(uds_probe_timeout.count());
  auto ready = ::poll(&fds, 1, timeout);
  if (ready == 0) {
    return true;
  }
  if (ready == -1) {
    emit_probe_error(path, source, dh, std::strerror(errno));
    return failure::promise();
  }
  auto error = int{0};
  auto error_size = socklen_t{sizeof(error)};
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_size) == -1) {
    emit_probe_error(path, source, dh, std::strerror(errno));
    return failure::promise();
  }
  if (error == 0) {
    return true;
  }
  if (error == ECONNREFUSED or error == ENOENT) {
    return false;
  }
  emit_probe_error(path, source, dh, std::strerror(error));
  return failure::promise();
}

} // namespace

UdsServerSocket::UdsServerSocket(Arc<folly::AsyncServerSocket> socket,
                                 folly::SocketAddress const& address,
                                 uint32_t listen_queue_depth)
  : socket_{std::move(socket)} {
  socket_->bind(address);
  socket_->listen(listen_queue_depth);
}

auto UdsServerSocket::accept() -> Task<Box<folly::coro::Transport>> {
  co_await folly::coro::co_safe_point;
  auto baton = folly::coro::Baton{};
  auto callback = UdsAcceptCallback{baton, socket_};
  socket_->addAcceptCallback(&callback, nullptr);
  auto unregister_callback = detail::scope_guard{[this, &callback] noexcept {
    if (callback.registered) {
      socket_->pauseAccepting();
      try {
        socket_->removeAcceptCallback(&callback, nullptr);
      } catch (std::exception const&) {
        // AsyncServerSocket may remove the callback as part of completing an
        // accept. The guard still has to run on cancellation, where the stack
        // callback must not remain registered.
      }
      callback.registered = false;
    }
  }};
  socket_->startAccepting();
  auto cancel_token = co_await folly::coro::co_current_cancellation_token;
  auto cancellation_callback = folly::CancellationCallback{
    cancel_token,
    [&baton] {
      baton.post();
    },
  };
  co_await baton;
  if (callback.error) {
    co_yield folly::coro::co_error(std::move(callback.error));
  }
  if (callback.accept_fd == -1 and cancel_token.isCancellationRequested()) {
    socket_->stopAccepting();
    co_yield folly::coro::co_stopped_may_throw;
  }
  co_return Box<folly::coro::Transport>{
    std::in_place,
    socket_->getEventBase(),
    folly::AsyncSocket::newSocket(
      socket_->getEventBase(),
      folly::NetworkSocket::fromFd(callback.accept_fd)),
  };
}

auto UdsServerSocket::close() noexcept -> void {
  socket_->stopAccepting();
}

auto make_uds_socket_address(std::string const& path, location source,
                             diagnostic_handler& dh)
  -> failure_or<folly::SocketAddress> {
  if (path.size() > max_uds_path_size()) {
    diagnostic::error("UNIX domain socket path is too long")
      .primary(source)
      .note("path length: {}, maximum: {}", path.size(), max_uds_path_size())
      .emit(dh);
    return failure::promise();
  }
  auto result = folly::SocketAddress{};
  try {
    result.setFromPath(path);
  } catch (std::exception const& ex) {
    diagnostic::error("invalid UNIX domain socket path")
      .primary(source)
      .note("reason: {}", ex.what())
      .emit(dh);
    return failure::promise();
  }
  return result;
}

auto prepare_uds_listen_path(std::string const& path, location source,
                             diagnostic_handler& dh) -> failure_or<void> {
  if (path.size() > max_uds_path_size()) {
    diagnostic::error("UNIX domain socket path is too long")
      .primary(source)
      .note("path length: {}, maximum: {}", path.size(), max_uds_path_size())
      .emit(dh);
    return failure::promise();
  }
  auto ec = std::error_code{};
  auto status = std::filesystem::status(path, ec);
  if (ec and ec != std::errc::no_such_file_or_directory) {
    diagnostic::error("failed to inspect UNIX domain socket path")
      .primary(source)
      .note("path: {}", path)
      .note("reason: {}", ec.message())
      .emit(dh);
    return failure::promise();
  }
  auto parent = std::filesystem::path{path}.parent_path();
  if (not parent.empty()) {
    auto parent_status = std::filesystem::status(parent, ec);
    if (ec) {
      diagnostic::error("failed to inspect UNIX domain socket directory")
        .primary(source)
        .note("path: {}", parent.string())
        .note("reason: {}", ec.message())
        .emit(dh);
      return failure::promise();
    }
    if (not std::filesystem::is_directory(parent_status)) {
      diagnostic::error("UNIX domain socket directory is not available")
        .primary(source)
        .note("path: {}", parent.string())
        .hint("create the directory or choose another path")
        .emit(dh);
      return failure::promise();
    }
  }
  if (not ec and status.type() != std::filesystem::file_type::not_found) {
    if (status.type() != std::filesystem::file_type::socket) {
      diagnostic::error("UNIX domain socket path already exists")
        .primary(source)
        .note("path: {}", path)
        .hint("remove the existing non-socket file or choose another path")
        .emit(dh);
      return failure::promise();
    }
    auto active = uds_path_has_listener(path, source, dh);
    if (not active) {
      return failure::promise();
    }
    if (*active) {
      diagnostic::error("UNIX domain socket path is already in use")
        .primary(source)
        .hint("stop the existing server or choose another path")
        .emit(dh);
      return failure::promise();
    }
    std::filesystem::remove(path, ec);
    if (ec) {
      diagnostic::error("failed to remove stale UNIX domain socket")
        .primary(source)
        .note("path: {}", path)
        .note("reason: {}", ec.message())
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

} // namespace tenzir
