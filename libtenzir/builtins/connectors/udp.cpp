//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

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

  friend auto inspect(auto& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.udp.loader_args")
      .fields(f.field("url", x.url), f.field("connect", x.connect),
              f.field("insert_newlines", x.insert_newlines));
  }
};

struct saver_args {
  std::string url = {};

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.udp.saver_args")
      .fields(f.field("url", x.url));
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
  auto enable = int{1};
  if (::setsockopt(*socket.fd, SOL_SOCKET, SO_REUSEADDR, &enable,
                   sizeof(enable))
      < 0) {
    diagnostic::error("could not set socket to SO_REUSEADDR")
      .note(detail::describe_errno())
      .emit(ctrl.diagnostics());
    co_return;
  }
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
  if (auto err = detail::make_nonblocking(*socket.fd)) {
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
    TENZIR_TRACE("polling socket");
    auto ready = detail::rpoll(*socket.fd, usec);
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
    TENZIR_TRACE("got {} bytes", received_bytes);
    TENZIR_ASSERT(received_bytes
                  < detail::narrow_cast<ssize_t>(buffer.size()) - 1);
    // Append a newline unless we have one already.
    if (args.insert_newlines && buffer[received_bytes - 1] != '\n') {
      buffer[received_bytes++] = '\n';
    }
    co_yield chunk::copy(as_bytes(buffer).subspan(0, received_bytes));
  }
}

class loader final : public plugin_loader {
public:
  loader() = default;

  explicit loader(loader_args args) : args_{std::move(args)} {
    // nop
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return udp_loader_impl(ctrl, args_);
  }

  auto name() const -> std::string override {
    return "udp";
  }

  friend auto inspect(auto& f, loader& x) -> bool {
    return f.object(x).pretty_name("loader").fields(f.field("args", x.args_));
  }

private:
  loader_args args_;
};

class saver final : public plugin_saver {
public:
  saver() = default;

  explicit saver(saver_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto endpoint = socket_endpoint::parse(args_.url);
    if (not endpoint) {
      return diagnostic::error("invalid UDP endpoint")
        .note("{}", endpoint.error())
        .to_error();
    }
    auto socket = tenzir::socket{*endpoint};
    if (not socket) {
      return diagnostic::error("failed to create UDP socket")
        .note(detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .to_error();
    };
    TENZIR_DEBUG("connecting to {}", args_.url);
    int enable = 1;
    if (::setsockopt(*socket.fd, SOL_SOCKET, SO_REUSEADDR, &enable,
                     sizeof(enable))
        < 0) {
      return diagnostic::error("could not set socket to SO_REUSEADDR")
        .note(detail::describe_errno())
        .to_error();
    }
    if (socket.connect(*endpoint) < 0) {
      return diagnostic::error("failed to connect to socket")
        .note(detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .to_error();
    }
    return [&ctrl, socket = std::make_shared<tenzir::socket>(
                     std::move(socket))](chunk_ptr chunk) mutable {
      if (not chunk || chunk->size() == 0) {
        return;
      }
      // If we exceed the maximum UDP datagram size of 65,535 we are in trouble.
      if (chunk->size() > 65'535) {
        diagnostic::error("chunk exceeded maximum size of 65,535 bytes")
          .emit(ctrl.diagnostics());
        return;
      }
      auto sent_bytes = socket->send(as_bytes(chunk));
      if (sent_bytes == -1) {
        diagnostic::error("failed to send data over UDP socket")
          .note(detail::describe_errno())
          .emit(ctrl.diagnostics());
        return;
      }
      TENZIR_TRACE("sent {} bytes", sent_bytes);
      if (detail::narrow_cast<size_t>(sent_bytes) < chunk->size()) {
        diagnostic::warning("incomplete UDP send operation")
          .note("got {} bytes but sent only {}", sent_bytes, chunk->size())
          .emit(ctrl.diagnostics());
      }
    };
  }

  auto name() const -> std::string override {
    return "udp";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, saver& x) -> bool {
    return f.object(x).pretty_name("saver").fields(f.field("args", x.args_));
  }

private:
  saver_args args_;
};

class plugin final : public virtual loader_plugin<loader>,
                     public virtual saver_plugin<saver> {
public:
  auto name() const -> std::string override {
    return "udp";
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
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
    return std::make_unique<loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto endpoint = located<std::string>{};
    auto args = saver_args{};
    parser.add(endpoint, "<endpoint>");
    parser.parse(p);
    if (not endpoint.inner.starts_with("udp://")) {
      args.url = fmt::format("udp://{}", endpoint.inner);
    } else {
      args.url = std::move(endpoint.inner);
    }
    return std::make_unique<saver>(std::move(args));
  }
};

class load_plugin final
  : public virtual operator_plugin2<loader_adapter<loader>> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = loader_args{};
    auto parser = argument_parser2::operator_(name());
    parser.add(args.url, "<endpoint>");
    parser.add("connect", args.connect);
    parser.add("insert_newlines", args.insert_newlines);
    TRY(parser.parse(inv, ctx));
    if (not args.url.starts_with("udp://")) {
      args.url.insert(0, "udp://");
    }
    return std::make_unique<loader_adapter<loader>>(loader{std::move(args)});
  }
};

class save_plugin final
  : public virtual operator_plugin2<saver_adapter<saver>> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = saver_args{};
    auto parser = argument_parser2::operator_(name());
    parser.add(args.url, "<endpoint>");
    TRY(parser.parse(inv, ctx));
    if (not args.url.starts_with("udp://")) {
      args.url.insert(0, "udp://");
    }
    return std::make_unique<saver_adapter<saver>>(saver{std::move(args)});
  }
};
} // namespace

} // namespace tenzir::plugins::udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::udp::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::udp::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::udp::save_plugin)
