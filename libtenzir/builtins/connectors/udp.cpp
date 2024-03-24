//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/plugin.hpp>

#include <arpa/inet.h>
#include <arrow/util/uri.h>
#include <caf/uri.hpp>
#include <sys/socket.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::udp {

namespace {

struct loader_args {
  std::string url = {};
  bool connect = {};
  bool insert_newlines = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.udp.loader_args")
      .fields(f.field("url", x.url), f.field("connect", x.connect),
              f.field("insert_newlines", x.insert_newlines));
  }
};

enum class socket_type {
  invalid,
  tcp,
  udp,
};

/// Small wrapper to facilitate interacting with socket addreses.
struct socket_endpoint {
  /// Parses a URL-like string into a socket endpoint, e.g., tcp://localhost:42
  /// or udp://1.2.3.4
  static auto parse(std::string_view url) -> caf::expected<socket_endpoint> {
    auto uri = arrow::internal::Uri{};
    if (not uri.Parse(std::string{url}).ok()) {
      return ec::parse_error;
    }
    auto result = socket_endpoint{};
    result.addr.sin_family = AF_INET;
    if (uri.scheme() == "udp") {
      result.type = socket_type::udp;
    } else if (uri.scheme() == "tcp") {
      result.type = socket_type::tcp;
    } else {
      return caf::make_error(ec::parse_error, "invalid URL scheme");
    }
    // Parse host.
    // TODO: use getaddrinfo
    if (inet_pton(AF_INET, uri.host().c_str(), &result.addr.sin_addr) < 0) {
      return caf::make_error(ec::parse_error, "failed to resolve host");
    }
    // Parse port.
    if (uri.port() > 0) {
      result.addr.sin_port = htons(uri.port());
    } else {
      return caf::make_error(ec::parse_error, "missing port");
    }
    return result;
  }

  socket_endpoint() {
    std::memset(&addr, 0, sizeof(addr));
  }

  auto as_sockaddr() -> sockaddr* {
    return reinterpret_cast<sockaddr*>(&addr);
  }

  sockaddr_in addr = {};
  socket_type type = {};
};

/// Minimalistic RAII wrapper around a plain socket.
struct socket {
  socket() = default;

  explicit socket(socket_type type) {
    switch (type) {
      case socket_type::invalid:
        fd = -1;
        break;
      case socket_type::udp:
        fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        break;
      case socket_type::tcp:
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        break;
    }
  }

  ~socket() {
    if (*this) {
      ::close(fd);
    }
  }

  explicit operator bool() const {
    return fd >= 0;
  }

  auto connect(socket_endpoint endpoint) {
    auto* addr = endpoint.as_sockaddr();
    constexpr socklen_t addr_len = sizeof(sockaddr_in);
    return ::connect(fd, addr, addr_len);
  }

  auto bind(socket_endpoint endpoint) {
    auto* addr = endpoint.as_sockaddr();
    constexpr socklen_t addr_len = sizeof(sockaddr_in);
    return ::bind(fd, addr, addr_len);
  }

  auto recv(std::span<std::byte> buffer, int flags = 0) {
    return ::recv(fd, buffer.data(), buffer.size(), flags);
  }

  auto recvfrom(std::span<std::byte> buffer, socket_endpoint& endpoint,
                int flags = 0) {
    auto* addr = endpoint.as_sockaddr();
    socklen_t addr_len = sizeof(sockaddr_in);
    return ::recvfrom(fd, buffer.data(), buffer.size(), flags, addr, &addr_len);
  }

  int fd = -1;
  socket_type type = socket_type::invalid;
};

auto udp_loader_impl(operator_control_plane& ctrl, loader_args args)
  -> generator<chunk_ptr> {
  auto sock = socket{socket_type::udp};
  if (not sock) {
    diagnostic::error("failed to create UDP socket")
      .note("error: {}", detail::describe_errno())
      .emit(ctrl.diagnostics());
    co_return;
  };
  // A UDP packet contains its length as 16-bit field in the header, giving rise
  // to packets sized up to 65,535 bytes (including the header). When we go
  // over IPv4, we have a limit of 65,507 bytes (65,535 bytes − 8-byte UDP
  // header − 20-byte IP header). At the moment we are not supporting IPv6
  // jumbograms, which in theory get up to 2^32 - 1 bytes.
  auto buffer = std::array<char, 65'536>{};
  auto endpoint = socket_endpoint::parse(args.url);
  if (not endpoint) {
    diagnostic::error("invalid UDP endpoint")
      .note("{}", endpoint.error())
      .emit(ctrl.diagnostics());
    co_return;
  }
  if (args.connect) {
    TENZIR_DEBUG("connecting to {}", args.url);
    if (sock.connect(*endpoint) < 0) {
      diagnostic::error("failed to connect to socket")
        .note("error: {}", detail::describe_errno())
        .emit(ctrl.diagnostics());
      co_return;
    }
  } else {
    TENZIR_DEBUG("binding to {}", args.url);
    if (sock.bind(*endpoint) < 0) {
      diagnostic::error("failed to bind to socket")
        .note("error: {}", detail::describe_errno())
        .emit(ctrl.diagnostics());
      co_return;
    }
  }
  while (true) {
    TENZIR_DEBUG("receiving bytes");
    auto received_bytes = sock.recvfrom(as_writeable_bytes(buffer), *endpoint);
    if (received_bytes < 0) {
      diagnostic::error("failed to receive data from socket")
        .note("error: {}", detail::describe_errno())
        .emit(ctrl.diagnostics());
      co_return;
    }
    TENZIR_DEBUG("got {} bytes", received_bytes);
    TENZIR_ASSERT(received_bytes
                  < detail::narrow_cast<ssize_t>(buffer.size()) - 1);
    // Append a newline unless we have one already.
    if (args.insert_newlines && buffer[received_bytes - 1] != '\n') {
      buffer[received_bytes++] = '\n';
    }
    co_yield chunk::copy(as_bytes(buffer).subspan(0, received_bytes));
  }
}

class udp_loader final : public plugin_loader {
public:
  udp_loader() = default;

  explicit udp_loader(loader_args args) : args_{std::move(args)} {
    // nop
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return udp_loader_impl(ctrl, args_);
  }

  auto name() const -> std::string override {
    return "udp";
  }

  friend auto inspect(auto& f, udp_loader& x) -> bool {
    return f.object(x)
      .pretty_name("udp_loader")
      .fields(f.field("args", x.args_));
  }

private:
  loader_args args_;
};

class plugin final : public virtual loader_plugin<udp_loader> {
public:
  auto name() const -> std::string override {
    return "udp";
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto endpoint = located<std::string>{};
    auto args = loader_args{};
    parser.add(endpoint, "<endpoint>");
    parser.add("-c,--connect", args.connect);
    parser.add("-n,--insert-newlines", args.insert_newlines);
    parser.parse(p);
    if (not endpoint.inner.starts_with("udp://")) {
      args.url = fmt::format("udp://{}", endpoint.inner);
    } else {
      args.url = std::move(endpoint.inner);
    }
    return std::make_unique<udp_loader>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::udp::plugin)
