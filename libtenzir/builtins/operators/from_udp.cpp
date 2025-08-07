//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/socket.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arpa/inet.h>
#include <arrow/util/uri.h>
#include <caf/uri.hpp>
#include <sys/socket.h>
#include <sys/types.h>

#include <netdb.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::from_udp {

namespace {

struct args {
  located<std::string> endpoint;
  bool resolve_hostnames = false;

  friend auto inspect(auto& f, args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.from_udp.args")
      .fields(f.field("endpoint", x.endpoint.inner),
              f.field("resolve_hostnames", x.resolve_hostnames));
  }
};

class from_udp_operator final : public crtp_operator<from_udp_operator> {
public:
  from_udp_operator() = default;

  explicit from_udp_operator(args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // A UDP packet contains its length as 16-bit field in the header, giving
    // rise to packets sized up to 65,535 bytes (including the header). When we
    // go over IPv4, we have a limit of 65,507 bytes (65,535 bytes − 8-byte UDP
    // header − 20-byte IP header). At the moment we are not supporting IPv6
    // jumbograms, which in theory get up to 2^32 - 1 bytes.
    auto buffer = std::array<char, 65'536>{};
    auto endpoint = socket_endpoint::parse(args_.endpoint.inner);
    if (not endpoint) {
      diagnostic::error("invalid UDP endpoint")
        .primary(args_.endpoint, "{}", endpoint.error())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto socket = tenzir::socket{*endpoint};
    if (not socket) {
      diagnostic::error("failed to create UDP socket")
        .primary(args_.endpoint, detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto enable = int{1};
    if (::setsockopt(*socket.fd, SOL_SOCKET, SO_REUSEADDR, &enable,
                     sizeof(enable))
        < 0) {
      diagnostic::error("could not set socket to SO_REUSEADDR")
        .primary(args_.endpoint, detail::describe_errno())
        .emit(ctrl.diagnostics());
      co_return;
    }
    TENZIR_DEBUG("binding to {}", args_.endpoint.inner);
    if (socket.bind(*endpoint) < 0) {
      diagnostic::error("failed to bind to socket")
        .primary(args_.endpoint, detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .emit(ctrl.diagnostics());
      co_return;
    }
    // We're using a nonblocking socket and polling because blocking recvfrom(2)
    // doesn't deliver the data fast enough. We were always one datagram behind.
    if (auto err = detail::make_nonblocking(*socket.fd)) {
      diagnostic::error("failed to make socket nonblocking")
        .primary(args_.endpoint, detail::describe_errno())
        .note("{}", err)
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Define schema for output events
    auto output_type = type{
      "tenzir.from_udp",
      record_type{
        {"data", blob_type{}},
        {"peer",
         record_type{
           {"ip", ip_type{}},
           {"port", uint64_type{}},
           {"hostname", string_type{}},
         }},
      },
    };
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
          .primary(args_.endpoint, detail::describe_errno())
          .note("{}", ready.error())
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (not *ready) {
        co_yield {};
        continue;
      }
      // Create a socket_endpoint to receive sender information
      auto sender_endpoint = socket_endpoint{};
      // Initialize the variant to hold either IPv4 or IPv6 address
      if (endpoint->addr.is_v4()) {
        sender_endpoint.sock_addr = sockaddr_in{};
      } else {
        sender_endpoint.sock_addr = sockaddr_in6{};
      }
      auto received_bytes
        = socket.recvfrom(as_writeable_bytes(buffer), sender_endpoint);
      if (received_bytes < 0) {
        diagnostic::error("failed to receive data from socket")
          .primary(args_.endpoint, detail::describe_errno())
          .emit(ctrl.diagnostics());
        co_return;
      }
      // Extract peer information from the sockaddr structure
      auto peer_ip = ip{};
      auto peer_port = uint64_t{0};
      std::optional<std::string> peer_hostname;
      if (std::holds_alternative<sockaddr_in>(sender_endpoint.sock_addr)) {
        const auto& addr_in = std::get<sockaddr_in>(sender_endpoint.sock_addr);
        peer_ip = ip::v4(detail::to_host_order(addr_in.sin_addr.s_addr));
        peer_port = ntohs(addr_in.sin_port);
      } else if (std::holds_alternative<sockaddr_in6>(
                   sender_endpoint.sock_addr)) {
        const auto& addr_in6
          = std::get<sockaddr_in6>(sender_endpoint.sock_addr);
        peer_ip = ip::v6(as_bytes<16>(&addr_in6.sin6_addr, 16));
        peer_port = ntohs(addr_in6.sin6_port);
      }
      TENZIR_TRACE("got {} bytes from {}:{}", received_bytes, peer_ip,
                   peer_port);
      TENZIR_ASSERT(received_bytes
                    < detail::narrow_cast<ssize_t>(buffer.size()));
      // Try to resolve hostname (optional, don't fail on error)
      if (args_.resolve_hostnames) {
        auto host = std::array<char, NI_MAXHOST>{};
        if (::getnameinfo(sender_endpoint.as_sock_addr(),
                          sender_endpoint.sock_addr_len(), host.data(),
                          host.size(), nullptr, 0, NI_NAMEREQD)
            == 0) {
          peer_hostname = std::string{host.data()};
        }
      }
      // Build the output event
      auto builder = series_builder{output_type};
      auto event = builder.record();
      // Add data field
      auto data_bytes = as_bytes(buffer).subspan(0, received_bytes);
      event.field("data").data(blob{data_bytes.begin(), data_bytes.end()});
      // Add peer record
      auto peer = event.field("peer").record();
      peer.field("ip").data(peer_ip);
      peer.field("port").data(peer_port);
      if (peer_hostname) {
        peer.field("hostname").data(*peer_hostname);
      } else {
        peer.field("hostname").null();
      }
      for (auto&& slice : builder.finish_as_table_slice()) {
        co_yield std::move(slice);
      }
    }
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "from_udp";
  }

  friend auto inspect(auto& f, from_udp_operator& x) -> bool {
    return f.object(x)
      .pretty_name("from_udp_operator")
      .fields(f.field("args", x.args_));
  }

private:
  args args_;
};

class plugin final : public virtual operator_plugin2<from_udp_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_udp::args{};
    auto parser = argument_parser2::operator_(name());
    parser.positional("endpoint", args.endpoint);
    parser.named("resolve_hostnames", args.resolve_hostnames);
    TRY(parser.parse(inv, ctx));
    if (not args.endpoint.inner.starts_with("udp://")) {
      args.endpoint.inner.insert(0, "udp://");
    }
    return std::make_unique<from_udp_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::from_udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_udp::plugin)
