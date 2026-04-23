//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/channel.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/result.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/socket.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <arpa/inet.h>
#include <arrow/util/uri.h>
#include <arrow/util/utf8.h>
#include <caf/uri.hpp>
#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/coro/Sleep.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <deque>
#include <netdb.h>
#include <optional>
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

// A UDP packet carries its own length and is capped at 65,535 bytes including
// headers. We size the read buffer to accommodate the full userspace payload.
constexpr auto udp_buffer_size = size_t{65'536};

// Allow short bursts of datagrams without forcing immediate backpressure on the
// socket, while still keeping the queued working set bounded.
constexpr auto message_queue_capacity = uint32_t{256};

struct FromUdpArgs {
  located<std::string> endpoint;
  bool resolve_hostnames = false;
  bool binary = false;
};

struct Datagram {
  Datagram(chunk_ptr payload, ip peer_ip, uint16_t peer_port)
    : payload{std::move(payload)}, peer_ip{peer_ip}, peer_port{peer_port} {
  }

  chunk_ptr payload;
  ip peer_ip;
  uint16_t peer_port;
};

enum class ErrorStage {
  startup,
  runtime,
};

struct Error {
  Error(ErrorStage stage, std::string detail)
    : stage{stage}, detail{std::move(detail)} {
  }

  ErrorStage stage;
  std::string detail;
};

struct Flush {
  explicit Flush(uint64_t generation) : generation{generation} {
  }

  uint64_t generation;
};

using Message = variant<Datagram, Error, Flush>;

auto reverse_lookup(ip const& peer_ip, uint16_t peer_port)
  -> Option<std::string> {
  auto host = std::array<char, NI_MAXHOST>{};
  if (peer_ip.is_v4()) {
    auto addr = sockaddr_in{};
    auto err = convert(peer_ip, addr);
    TENZIR_ASSERT(err.empty());
    addr.sin_port = htons(peer_port);
    if (::getnameinfo(reinterpret_cast<sockaddr*>(&addr), sizeof(addr),
                      host.data(), host.size(), nullptr, 0, NI_NAMEREQD)
        == 0) {
      return std::string{host.data()};
    }
    return None{};
  }
  auto addr = sockaddr_in6{};
  auto err = convert(peer_ip, addr);
  TENZIR_ASSERT(err.empty());
  addr.sin6_port = htons(peer_port);
  if (::getnameinfo(reinterpret_cast<sockaddr*>(&addr), sizeof(addr),
                    host.data(), host.size(), nullptr, 0, NI_NAMEREQD)
      == 0) {
    return std::string{host.data()};
  }
  return None{};
}

// Callback and reader coroutine both run on the EventBase thread, so access
// to the members below is unsynchronized by design.
struct SocketReadCallback final : folly::AsyncUDPSocket::ReadCallback {
  // Pause when `pending` grows past the high-water mark; resume once it drains
  // back to the low-water mark. Hysteresis avoids `epoll_ctl` thrash under
  // routine scheduler jitter.
  static constexpr size_t pending_high_water = 1024;
  static constexpr size_t pending_low_water = 256;

  std::deque<Message> pending;
  Notify notify;
  MetricsCounter bytes_read_counter;
  folly::AsyncUDPSocket* socket = nullptr;
  bool paused = false;
  bool done = false;

  auto getReadBuffer(void** buf, size_t* len) noexcept -> void override {
    *buf = buffer_.data();
    *len = buffer_.size();
  }

  void
  onDataAvailable(const folly::SocketAddress& client, size_t len,
                  bool truncated, OnDataAvailableParams) noexcept override {
    // The buffer is sized to fit any valid UDP datagram payload (max 65,527
    // bytes), so the kernel should never truncate.
    TENZIR_ASSERT(not truncated);
    auto storage = sockaddr_storage{};
    auto addr_len = client.getAddress(&storage);
    TENZIR_ASSERT(addr_len == sizeof(sockaddr_in)
                  or addr_len == sizeof(sockaddr_in6));
    auto peer_ip = ip{};
    if (client.getFamily() == AF_INET) {
      auto err
        = convert(*reinterpret_cast<sockaddr_in const*>(&storage), peer_ip);
      TENZIR_ASSERT(err.empty());
    } else {
      auto err
        = convert(*reinterpret_cast<sockaddr_in6 const*>(&storage), peer_ip);
      TENZIR_ASSERT(err.empty());
    }
    auto payload = chunk::copy(
      std::span{reinterpret_cast<std::byte const*>(buffer_.data()), len});
    pending.emplace_back(Datagram{
      std::move(payload),
      peer_ip,
      client.getPort(),
    });
    bytes_read_counter.add(len);
    if (not paused and pending.size() >= pending_high_water) {
      socket->pauseRead();
      paused = true;
    }
    notify.notify_one();
  }

  void onReadError(folly::AsyncSocketException const& ex) noexcept override {
    done = true;
    pending.emplace_back(Error{ErrorStage::runtime, ex.what()});
    notify.notify_one();
  }

  void onReadClosed() noexcept override {
    done = true;
    notify.notify_one();
  }

private:
  std::array<std::byte, udp_buffer_size> buffer_;
};

class FromUdp final : public Operator<void, table_slice> {
public:
  explicit FromUdp(FromUdpArgs args)
    : FromUdp{std::move(args), channel<Message>(message_queue_capacity)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // Pin to one specific EventBase from the IO pool: the socket and the
    // coroutine must run on the same thread so that AsyncUDPSocket's
    // `dcheckIsInEventBaseThread` holds and access to `SocketReadCallback`'s
    // members needs no synchronization.
    evb_ = folly::getKeepAliveToken(ctx.io_executor()->getEventBase());
    // The underlying `getaddrinfo` call is blocking.
    auto url = args_.endpoint.inner;
    auto bind_address = co_await spawn_blocking(
      [url = std::move(url)] -> Result<folly::SocketAddress, std::string> {
        auto result = folly::SocketAddress{};
        try {
          result.setFromHostPort(url);
        } catch (std::exception const& ex) {
          return Err{std::string{ex.what()}};
        }
        return result;
      });
    if (bind_address.is_err()) {
      diagnostic::error("invalid UDP endpoint")
        .primary(args_.endpoint, "{}", std::move(bind_address).unwrap_err())
        .emit(ctx);
      message_sender_ = None{};
      done_ = true;
      co_return;
    }
    // A truly label-free counter is not possible with the current metrics API:
    // `make_counter` always requires exactly one label pair.
    auto bytes_read_counter
      = ctx.make_counter(MetricsLabel{"operator", "from_udp"},
                         MetricsDirection::read, MetricsVisibility::external_);
    TENZIR_ASSERT(message_sender_);
    ctx.spawn_task(folly::coro::co_withExecutor(
      evb_, read_loop(*evb_, std::move(bind_address).unwrap(), *message_sender_,
                      std::move(bytes_read_counter))));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    auto message = co_await message_receiver_.recv();
    // Every path that drops `message_sender_` also sets `done_ = true`, so
    // once we get here the channel must still have a live sender.
    TENZIR_ASSERT(message);
    co_return std::move(*message);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](Datagram datagram) -> Task<void> {
        auto hostname = Option<std::string>{};
        if (args_.resolve_hostnames) {
          // This is not very efficient can could be a bottleneck when enabled.
          hostname = co_await spawn_blocking([datagram] -> Option<std::string> {
            return reverse_lookup(datagram.peer_ip, datagram.peer_port);
          });
        }
        auto bytes = as_bytes(datagram.payload);
        auto string = std::string_view{
          reinterpret_cast<char const*>(bytes.data()), bytes.size()};
        if (not args_.binary and not arrow::util::ValidateUTF8(string)) {
          diagnostic::warning("message is not valid UTF-8")
            .primary(args_.endpoint)
            .note("peer: {}:{}", datagram.peer_ip, datagram.peer_port)
            .hint("use `binary=true` to accept non-UTF8 data")
            .emit(ctx);
          co_return;
        }
        auto event = builder_.record();
        if (args_.binary) {
          event.field("data").data(bytes);
        } else {
          event.field("data").data(string);
        }
        auto peer = event.field("peer").record();
        peer.field("ip").data(datagram.peer_ip);
        peer.field("port").data(int64_t{datagram.peer_port});
        if (hostname) {
          peer.field("hostname").data(std::move(*hostname));
        }
        if (builder_.length() == 1) {
          schedule_batch_flush(ctx);
        }
        if (builder_.length()
            >= static_cast<int64_t>(defaults::import::table_slice_size)) {
          co_await flush_builder(push);
        }
      },
      [&](Flush flush) -> Task<void> {
        if (builder_.length() == 0 or flush.generation != batch_generation_) {
          co_return;
        }
        co_await flush_builder(push);
      },
      [&](Error error) -> Task<void> {
        cancel_batch_flush();
        message_sender_ = None{};
        done_ = true;
        // No need to flush buffered events on errors as we are shutting down.
        switch (error.stage) {
          case ErrorStage::startup:
            diagnostic::error("from_udp: failed to start UDP socket")
              .primary(args_.endpoint)
              .note("{}", error.detail)
              .emit(ctx);
            co_return;
          case ErrorStage::runtime:
            diagnostic::error("from_udp: failed to receive data from socket")
              .primary(args_.endpoint)
              .note("{}", error.detail)
              .emit(ctx);
            co_return;
        }
        TENZIR_UNREACHABLE();
      });
  }

  auto stop(OpCtx&) -> Task<void> override {
    // Hard teardown: we drop any buffered data — datagrams still queued in
    // `callback.pending`, messages in flight in the channel, and the
    // unflushed events in `builder_`. UDP is lossy by design, so this is
    // acceptable on shutdown.
    cancel_batch_flush();
    message_sender_ = None{};
    done_ = true;
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
  explicit FromUdp(FromUdpArgs args,
                   std::tuple<Sender<Message>, Receiver<Message>> ch)
    : args_{std::move(args)},
      builder_{make_output_type(args_)},
      message_sender_{std::move(std::get<0>(ch))},
      message_receiver_{std::move(std::get<1>(ch))} {
  }

  static auto make_output_type(FromUdpArgs const& args) -> type {
    auto peer_record_fields = std::vector<record_type::field_view>{
      {"ip", ip_type{}},
      {"port", int64_type{}},
    };
    if (args.resolve_hostnames) {
      peer_record_fields.push_back({"hostname", string_type{}});
    }
    return type{
      "tenzir.from_udp",
      record_type{
        {"data", args.binary ? type{blob_type{}} : type{string_type{}}},
        {"peer", record_type{peer_record_fields}},
      },
    };
  }

  static auto
  read_loop(folly::EventBase& evb, folly::SocketAddress bind_address,
            Sender<Message> message_sender, MetricsCounter bytes_read_counter)
    -> Task<void> {
    auto socket = folly::AsyncUDPSocket{&evb};
    auto callback = SocketReadCallback{};
    callback.bytes_read_counter = std::move(bytes_read_counter);
    callback.socket = &socket;
    auto socket_guard = detail::scope_guard{[&]() noexcept {
      socket.pauseRead();
      socket.close();
    }};
    auto startup_error = Option<std::string>{};
    try {
      socket.setReuseAddr(true);
      socket.bind(bind_address);
      socket.resumeRead(&callback);
    } catch (std::exception const& ex) {
      startup_error = ex.what();
    }
    if (startup_error) {
      co_await message_sender.send(
        Error{ErrorStage::startup, std::move(*startup_error)});
      co_return;
    }
    while (true) {
      while (not callback.pending.empty()) {
        auto message = std::move(callback.pending.front());
        callback.pending.pop_front();
        co_await message_sender.send(std::move(message));
        if (callback.paused and not callback.done
            and callback.pending.size()
                  <= SocketReadCallback::pending_low_water) {
          callback.paused = false;
          socket.resumeRead(&callback);
        }
      }
      if (callback.done) {
        break;
      }
      co_await callback.notify.wait();
    }
  }

  static auto flush_batch_after(Sender<Message> message_sender,
                                uint64_t generation) -> Task<void> {
    co_await folly::coro::sleep(defaults::import::batch_timeout);
    co_await message_sender.send(Flush{generation});
  }

  auto schedule_batch_flush(OpCtx& ctx) -> void {
    cancel_batch_flush();
    batch_generation_ += 1;
    batch_flush_cancel_.emplace();
    auto token = batch_flush_cancel_->getToken();
    auto generation = batch_generation_;
    TENZIR_ASSERT(message_sender_);
    auto message_sender = *message_sender_;
    ctx.spawn_task(folly::coro::co_withCancellation(
      token,
      folly::coro::co_withExecutor(
        evb_, flush_batch_after(std::move(message_sender), generation))));
  }

  auto cancel_batch_flush() -> void {
    if (batch_flush_cancel_) {
      batch_flush_cancel_->requestCancellation();
      batch_flush_cancel_.reset();
    }
  }

  auto flush_builder(Push<table_slice>& push) -> Task<void> {
    if (builder_.length() == 0) {
      co_return;
    }
    cancel_batch_flush();
    co_await push(builder_.finish_assert_one_slice());
  }

  FromUdpArgs args_;
  series_builder builder_;
  folly::Executor::KeepAlive<folly::EventBase> evb_;
  Option<Sender<Message>> message_sender_;
  mutable Receiver<Message> message_receiver_;
  Option<folly::CancellationSource> batch_flush_cancel_;
  uint64_t batch_generation_ = 0;
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
