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
#include <tenzir/socket.hpp>

#include <arpa/inet.h>
#include <arrow/util/uri.h>
#include <caf/uri.hpp>
#include <sys/socket.h>
#include <sys/types.h>

#include <netdb.h>

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

auto udp_loader_impl(operator_control_plane& ctrl, loader_args args)
  -> generator<chunk_ptr> {
  // A UDP packet contains its length as 16-bit field in the header, giving
  // rise to packets sized up to 65,535 bytes (including the header). When we
  // go over IPv4, we have a limit of 65,507 bytes (65,535 bytes − 8-byte UDP
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
  auto socket = tenzir::socket{*endpoint};
  if (not socket) {
    diagnostic::error("failed to create UDP socket")
      .note(detail::describe_errno())
      .note("endpoint: {}", endpoint->addr)
      .emit(ctrl.diagnostics());
    co_return;
  };
  if (args.connect) {
    TENZIR_DEBUG("connecting to {}", args.url);
    if (socket.connect(*endpoint) < 0) {
      diagnostic::error("failed to connect to socket")
        .note(detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .emit(ctrl.diagnostics());
      co_return;
    }
  } else {
    TENZIR_DEBUG("binding to {}", args.url);
    if (socket.bind(*endpoint) < 0) {
      diagnostic::error("failed to bind to socket")
        .note(detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .emit(ctrl.diagnostics());
      co_return;
    }
  }
  // We're using a nonblocking socket and polling because blocking recvfrom(2)
  // doesn't deliver the data fast enough. We were always one datagram behind.
  if (auto err = detail::make_nonblocking(socket.fd)) {
    diagnostic::error("failed to make socket nonblocking")
      .note(detail::describe_errno())
      .note("{}", err)
      .emit(ctrl.diagnostics());
    co_return;
  }
  co_yield {};
  while (true) {
    constexpr auto poll_timeout = 500ms;
    constexpr auto usec
      = std::chrono::duration_cast<std::chrono::microseconds>(poll_timeout)
          .count();
    TENZIR_DEBUG("polling socket");
    auto ready = detail::rpoll(socket.fd, usec);
    if (not ready) {
      diagnostic::error("failed to poll socket")
        .note(detail::describe_errno())
        .note("{}", ready.error())
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (not *ready) {
      co_yield {};
      continue;
    }
    auto received_bytes
      = socket.recvfrom(as_writeable_bytes(buffer), *endpoint);
    if (received_bytes < 0) {
      diagnostic::error("failed to receive data from socket")
        .note(detail::describe_errno())
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
