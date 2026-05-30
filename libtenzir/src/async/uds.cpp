//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/uds.hpp"

#include "tenzir/detail/scope_guard.hpp"

#include <sys/socket.h>
#include <sys/un.h>

#include <cerrno>
#include <cstring>
#include <exception>
#include <filesystem>
#include <unistd.h>

namespace tenzir {

namespace {

auto max_uds_path_size() -> size_t {
  return sizeof(sockaddr_un{}.sun_path) - 1;
}

auto uds_path_has_listener(std::string const& path, location source,
                           diagnostic_handler& dh) -> failure_or<bool> {
  auto fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd == -1) {
    diagnostic::error("failed to probe UNIX domain socket")
      .primary(source)
      .note("path: {}", path)
      .note("reason: {}", std::strerror(errno))
      .emit(dh);
    return failure::promise();
  }
  auto close_fd = detail::scope_guard{[fd]() noexcept {
    ::close(fd);
  }};
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
  diagnostic::error("failed to probe UNIX domain socket")
    .primary(source)
    .note("path: {}", path)
    .note("reason: {}", std::strerror(errno))
    .emit(dh);
  return failure::promise();
}

} // namespace

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
