//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/box.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/socket.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <arpa/inet.h>
#include <arrow/util/uri.h>
#include <arrow/util/utf8.h>
#include <caf/uri.hpp>
#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <netdb.h>
#include <vector>

using namespace std::chrono_literals;

namespace tenzir::plugins::from_udp {

namespace {

struct args {
  located<std::string> endpoint;
  location operator_location;
  bool resolve_hostnames = false;
  bool binary = false;

  friend auto inspect(auto& f, args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.from_udp.args")
      .fields(f.field("endpoint", x.endpoint),
              f.field("operator_location", x.operator_location),
              f.field("resolve_hostnames", x.resolve_hostnames),
              f.field("binary", x.binary));
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
    co_yield {};
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
    if (auto err = detail::make_nonblocking(*socket.fd); err.valid()) {
      diagnostic::error("failed to make socket nonblocking")
        .primary(args_.endpoint, detail::describe_errno())
        .note("{}", err)
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Define schema for output events
    auto peer_record_fields = std::vector<record_type::field_view>{
      {"ip", ip_type{}},
      {"port", uint64_type{}},
    };
    if (args_.resolve_hostnames) {
      peer_record_fields.push_back({"hostname", string_type{}});
    }
    const auto output_type = type{
      "tenzir.from_udp",
      record_type{
        {"data", args_.binary ? type{blob_type{}} : type{string_type{}}},
        {"peer", record_type{std::move(peer_record_fields)}},
      },
    };
    auto builder = series_builder{output_type};
    auto last_yield_time = std::chrono::steady_clock::now();
    while (true) {
      constexpr auto usec
        = std::chrono::duration_cast<std::chrono::microseconds>(
            defaults::import::batch_timeout)
            .count();
      // Check if we should yield based on timeout
      const auto now = std::chrono::steady_clock::now();
      if (builder.length() > 0
          and now - last_yield_time >= defaults::import::batch_timeout) {
        co_yield builder.finish_assert_one_slice();
        last_yield_time = now;
      }
      const auto ready = detail::rpoll(*socket.fd, usec);
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
      const auto received_bytes
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
      auto peer_hostname = std::optional<std::string>{};
      if (auto* v4 = try_as<sockaddr_in>(sender_endpoint.sock_addr)) {
        peer_ip = ip::v4(detail::to_host_order(v4->sin_addr.s_addr));
        peer_port = ntohs(v4->sin_port);
      } else if (auto* v6 = try_as<sockaddr_in6>(sender_endpoint.sock_addr)) {
        peer_ip = ip::v6(as_bytes<16>(&v6->sin6_addr, 16));
        peer_port = ntohs(v6->sin6_port);
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
      auto event = builder.record();
      // Add data field
      const auto data_bytes = as_bytes(buffer).subspan(0, received_bytes);
      if (args_.binary) {
        event.field("data").data(blob{data_bytes.begin(), data_bytes.end()});
      } else {
        const auto valid = arrow::util::ValidateUTF8(
          reinterpret_cast<const unsigned char*>(data_bytes.data()),
          data_bytes.size());
        if (not valid) {
          diagnostic::warning("message is not valid UTF-8")
            .primary(args_.operator_location)
            .note("`data` will be dropped")
            .emit(ctrl.diagnostics());
          builder.remove_last();
          continue;
        } else {
          event.field("data").data(
            std::string{reinterpret_cast<const char*>(data_bytes.data()),
                        data_bytes.size()});
        }
      }
      // Add peer record
      auto peer = event.field("peer").record();
      peer.field("ip").data(peer_ip);
      peer.field("port").data(peer_port);
      if (args_.resolve_hostnames and peer_hostname) {
        peer.field("hostname").data(*peer_hostname);
      }
    }
  }

  auto detached() const -> bool override {
    return true;
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

class plugin final : public virtual operator_plugin2<from_udp_operator>,
                     public virtual OperatorPlugin {
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_udp::args{};
    args.operator_location = inv.self.get_location();
    auto parser = argument_parser2::operator_(name());
    parser.positional("endpoint", args.endpoint);
    parser.named("resolve_hostnames", args.resolve_hostnames);
    parser.named("binary", args.binary);
    TRY(parser.parse(inv, ctx));
    if (not args.endpoint.inner.starts_with("udp://")) {
      args.endpoint.inner.insert(0, "udp://");
    }
    return std::make_unique<from_udp_operator>(std::move(args));
  }

  auto describe() const -> Description override; // defined below
};

// -- new executor ------------------------------------------------------------

// Cap on how long a single `rpoll` poll can block before re-checking the stop
// flag and the batch deadline. Keeps shutdown latency bounded without burning
// CPU.
constexpr auto rpoll_tick = std::chrono::milliseconds{200};

// Number of completed table_slices the producer may queue before the consumer
// drains them. The producer flushes at most one slice per batch_timeout (1 s)
// or per `table_slice_size` datagrams, whichever comes first, so a small queue
// suffices.
constexpr auto slice_queue_capacity = uint32_t{8};

struct FromUdpArgs {
  located<std::string> endpoint;
  bool resolve_hostnames = false;
  bool binary = false;
};

class FromUdp final : public Operator<void, table_slice> {
public:
  explicit FromUdp(FromUdpArgs args) : args_{std::move(args)} {
    if (not args_.endpoint.inner.starts_with("udp://")) {
      args_.endpoint.inner.insert(0, "udp://");
    }
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto endpoint = socket_endpoint::parse(args_.endpoint.inner);
    if (not endpoint) {
      diagnostic::error("invalid UDP endpoint")
        .primary(args_.endpoint, "{}", endpoint.error())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto socket = tenzir::socket{*endpoint};
    if (not socket) {
      diagnostic::error("failed to create UDP socket")
        .primary(args_.endpoint, detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto enable = int{1};
    if (::setsockopt(*socket.fd, SOL_SOCKET, SO_REUSEADDR, &enable,
                     sizeof(enable))
        < 0) {
      diagnostic::error("could not set socket to SO_REUSEADDR")
        .primary(args_.endpoint, detail::describe_errno())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (socket.bind(*endpoint) < 0) {
      diagnostic::error("failed to bind to socket")
        .primary(args_.endpoint, detail::describe_errno())
        .note("endpoint: {}", endpoint->addr)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (auto err = detail::make_nonblocking(*socket.fd); err.valid()) {
      diagnostic::error("failed to make socket nonblocking")
        .primary(args_.endpoint, detail::describe_errno())
        .note("{}", err)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto peer_record_fields = std::vector<record_type::field_view>{
      {"ip", ip_type{}},
      {"port", uint64_type{}},
    };
    if (args_.resolve_hostnames) {
      peer_record_fields.push_back({"hostname", string_type{}});
    }
    output_type_ = type{
      "tenzir.from_udp",
      record_type{
        {"data", args_.binary ? type{blob_type{}} : type{string_type{}}},
        {"peer", record_type{std::move(peer_record_fields)}},
      },
    };
    socket_ = std::move(socket);
    is_v4_ = endpoint->addr.is_v4();
    diagnostics_ = &ctx.dh();
    ctx.spawn_task(producer_loop());
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    auto slice = co_await queue_->dequeue();
    co_return std::move(slice);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    auto* opt = result.try_as<Option<table_slice>>();
    if (not opt) {
      co_return;
    }
    if (not *opt) {
      // Producer finished (shutdown or fatal error). No more slices coming.
      done_ = true;
      co_return;
    }
    co_await push(std::move(**opt));
  }

  auto finalize(Push<table_slice>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    // Drain anything the producer enqueued before we noticed it was done.
    while (auto more = queue_->try_dequeue()) {
      if (not *more) {
        break;
      }
      co_await push(std::move(**more));
    }
    co_return FinalizeBehavior::done;
  }

  auto stop(OpCtx&) -> Task<void> override {
    stop_.store(true, std::memory_order_relaxed);
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde&) -> void override {
    // UDP datagrams in flight are inherently lossy across restarts; nothing
    // worth serialising here.
  }

private:
  // Drives the receive loop on the blocking executor. The lambda owns the
  // `series_builder` and the batch deadline; each `spawn_blocking` invocation
  // returns at most one completed `table_slice`, which the surrounding
  // coroutine then enqueues for the consumer.
  auto producer_loop() -> Task<void> {
    auto builder = series_builder{output_type_};
    auto first_packet_time = std::chrono::steady_clock::time_point{};
    while (not stop_.load(std::memory_order_relaxed)) {
      auto slice = co_await spawn_blocking([&] -> std::optional<table_slice> {
        return receive_until_slice(builder, first_packet_time);
      });
      if (not slice) {
        break;
      }
      co_await queue_->enqueue(Option<table_slice>{std::move(*slice)});
    }
    // Signal end of stream so the consumer can shut down cleanly.
    co_await queue_->enqueue(Option<table_slice>{None{}});
  }

  // Runs on the blocking executor. Polls the socket, appends datagrams to
  // `builder`, and returns the next completed slice (full or batch-timeout
  // expired). Returns `std::nullopt` on fatal error or shutdown with no
  // pending events.
  auto receive_until_slice(
    series_builder& builder,
    std::chrono::steady_clock::time_point& first_packet_time)
    -> std::optional<table_slice> {
    constexpr auto rpoll_us = static_cast<int>(
      std::chrono::duration_cast<std::chrono::microseconds>(rpoll_tick)
        .count());
    while (not stop_.load(std::memory_order_relaxed)) {
      // Honour the batch deadline first — we may have woken from rpoll just
      // because the timeout elapsed.
      if (builder.length() > 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now - first_packet_time >= defaults::import::batch_timeout) {
          return builder.finish_assert_one_slice();
        }
      }
      // Don't poll past the batch deadline.
      auto poll_us = rpoll_us;
      if (builder.length() > 0) {
        const auto deadline
          = first_packet_time + defaults::import::batch_timeout;
        const auto remaining
          = std::chrono::duration_cast<std::chrono::microseconds>(
              deadline - std::chrono::steady_clock::now())
              .count();
        poll_us
          = static_cast<int>(std::clamp<int64_t>(remaining, 0, rpoll_us));
      }
      const auto ready = detail::rpoll(*socket_.fd, poll_us);
      if (not ready) {
        diagnostic::error("from_udp: failed to poll socket")
          .primary(args_.endpoint, detail::describe_errno())
          .note("{}", ready.error())
          .emit(*diagnostics_);
        if (builder.length() > 0) {
          return builder.finish_assert_one_slice();
        }
        return std::nullopt;
      }
      if (not *ready) {
        continue;
      }
      auto sender = socket_endpoint{};
      if (is_v4_) {
        sender.sock_addr = sockaddr_in{};
      } else {
        sender.sock_addr = sockaddr_in6{};
      }
      auto buf = std::array<char, 65'536>{};
      const auto n = socket_.recvfrom(as_writeable_bytes(buf), sender);
      if (n < 0) {
        diagnostic::error("from_udp: failed to receive data from socket")
          .primary(args_.endpoint, detail::describe_errno())
          .emit(*diagnostics_);
        if (builder.length() > 0) {
          return builder.finish_assert_one_slice();
        }
        return std::nullopt;
      }
      TENZIR_ASSERT(n < detail::narrow_cast<ssize_t>(buf.size()));
      const auto bytes = as_bytes(buf).subspan(0, n);
      auto event = builder.record();
      if (args_.binary) {
        event.field("data").data(blob{bytes.begin(), bytes.end()});
      } else {
        const auto valid = arrow::util::ValidateUTF8(
          reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size());
        if (not valid) {
          diagnostic::warning("message is not valid UTF-8")
            .primary(args_.endpoint)
            .note("`data` will be dropped")
            .emit(*diagnostics_);
          builder.remove_last();
          continue;
        }
        event.field("data").data(std::string{
          reinterpret_cast<const char*>(bytes.data()), bytes.size()});
      }
      auto peer = event.field("peer").record();
      if (auto* v = try_as<sockaddr_in>(sender.sock_addr)) {
        peer.field("ip").data(
          ip::v4(detail::to_host_order(v->sin_addr.s_addr)));
        peer.field("port").data(uint64_t{ntohs(v->sin_port)});
      } else if (auto* v = try_as<sockaddr_in6>(sender.sock_addr)) {
        peer.field("ip").data(ip::v6(as_bytes<16>(&v->sin6_addr, 16)));
        peer.field("port").data(uint64_t{ntohs(v->sin6_port)});
      }
      if (args_.resolve_hostnames) {
        auto host = std::array<char, NI_MAXHOST>{};
        if (::getnameinfo(sender.as_sock_addr(), sender.sock_addr_len(),
                          host.data(), host.size(), nullptr, 0, NI_NAMEREQD)
            == 0) {
          peer.field("hostname").data(std::string{host.data()});
        }
      }
      if (builder.length() == 1) {
        first_packet_time = std::chrono::steady_clock::now();
      }
      if (builder.length()
          >= static_cast<int64_t>(defaults::import::table_slice_size)) {
        return builder.finish_assert_one_slice();
      }
    }
    // Shutdown requested. Flush any pending events.
    if (builder.length() > 0) {
      return builder.finish_assert_one_slice();
    }
    return std::nullopt;
  }

  FromUdpArgs args_;
  type output_type_;
  tenzir::socket socket_;
  bool is_v4_ = false;
  Atomic<bool> stop_{false};
  // The producer enqueues `Some(slice)` for each completed batch and
  // exactly one `None` to signal end of stream.
  mutable Box<folly::coro::BoundedQueue<Option<table_slice>>> queue_{
    std::in_place, slice_queue_capacity};
  // Captured from the OpCtx at start() so the producer task can emit
  // diagnostics from the blocking executor thread. The diagnostic_handler
  // outlives the operator's spawned tasks.
  diagnostic_handler* diagnostics_ = nullptr;
  bool done_ = false;
};

auto plugin::describe() const -> Description {
  auto d = Describer<FromUdpArgs, FromUdp>{};
  d.positional("endpoint", &FromUdpArgs::endpoint);
  d.named("resolve_hostnames", &FromUdpArgs::resolve_hostnames);
  d.named("binary", &FromUdpArgs::binary);
  return d.without_optimize();
}

} // namespace

} // namespace tenzir::plugins::from_udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_udp::plugin)
