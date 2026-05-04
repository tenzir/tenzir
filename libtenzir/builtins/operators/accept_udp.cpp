//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/channel.hpp>
#include <tenzir/async/dns.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/result.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/socket.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/util/utf8.h>
#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/coro/Sleep.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <array>
#include <chrono>
#include <deque>
#include <string_view>
#include <vector>

namespace tenzir::plugins::accept_udp {

namespace {

// A UDP packet carries its own length and is capped at 65,535 bytes including
// headers. We size the read buffer to accommodate the full userspace payload.
constexpr auto udp_buffer_size = size_t{65'536};

// Allow short bursts of datagrams without forcing immediate backpressure on the
// socket, while still keeping the queued working set bounded.
constexpr auto message_queue_capacity = uint32_t{256};

// Pause read when the pending queue reaches the high-water mark; resume once
// it drains back to the low-water mark. Hysteresis avoids `epoll_ctl` thrash
// under routine scheduler jitter.
constexpr auto pending_high_water = size_t{1024};
constexpr auto pending_low_water = size_t{256};

// Cap on datagrams packed into a single executor message. Must be at least
// `pending_high_water` so that one batch send can fully drain the pending
// queue after a pause; otherwise multiple sends per pause/resume cycle waste
// channel slots and amplify per-message overhead.
constexpr auto datagram_batch_size = size_t{1024};
static_assert(datagram_batch_size >= pending_high_water);

constexpr auto datagram_coalesce_delay = std::chrono::microseconds{100};
constexpr auto socket_receive_buffer_size = int{16 * 1024 * 1024};

struct AcceptUdpArgs {
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

struct DatagramBatch {
  explicit DatagramBatch(std::vector<Datagram> datagrams)
    : datagrams{std::move(datagrams)} {
  }

  std::vector<Datagram> datagrams;
};

struct Flush {
  explicit Flush(uint64_t generation) : generation{generation} {
  }

  uint64_t generation;
};

using PendingMessage = variant<Datagram, Error>;
using Message = variant<DatagramBatch, Error, Flush>;

// Callback and reader coroutine both run on the EventBase thread, so access
// to the members below is unsynchronized by design.
struct SocketReadCallback final : folly::AsyncUDPSocket::ReadCallback {
  std::deque<PendingMessage> pending;
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
    // Folly only calls `onReadClosed()` from `AsyncUDPSocket::close()` while a
    // read callback is still registered. Our teardown always does
    // `pauseRead()` before `close()`, so reaching this path means that teardown
    // ordering changed somewhere.
    TENZIR_UNREACHABLE();
  }

private:
  std::array<std::byte, udp_buffer_size> buffer_;
};

class AcceptUdp final : public Operator<void, table_slice> {
public:
  explicit AcceptUdp(AcceptUdpArgs args)
    : AcceptUdp{std::move(args), channel<Message>(message_queue_capacity)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // Pin to one specific EventBase from the IO pool: the socket and the
    // coroutine must run on the same thread so that AsyncUDPSocket's
    // `dcheckIsInEventBaseThread` holds and access to `SocketReadCallback`'s
    // members needs no synchronization.
    evb_ = folly::getKeepAliveToken(ctx.io_executor()->getEventBase());
    auto bind_endpoint
      = parse_socket_address(args_.endpoint.inner, SocketAddressKind::bind)
          .expect("accept_udp endpoint should be valid after "
                  "operator validation");
    auto bind_address
      = co_await forward_dns_.resolve_bind_address(std::move(bind_endpoint));
    if (bind_address.is_err()) {
      diagnostic::error("failed to resolve listen address")
        .primary(args_.endpoint)
        .note("reason: {}", std::move(bind_address).unwrap_err())
        .emit(ctx);
      message_sender_ = None{};
      co_return;
    }
    if (args_.resolve_hostnames) {
      if (auto error = reverse_dns_.startup_error()) {
        diagnostic::error("failed to initialize DNS resolver")
          .primary(args_.endpoint, "reason: {}", error->error)
          .emit(ctx);
        message_sender_ = None{};
        co_return;
      }
    }
    // A truly label-free counter is not possible with the current metrics API:
    // `make_counter` always requires exactly one label pair.
    auto bytes_read_counter
      = ctx.make_counter(MetricsLabel{"operator", "accept_udp"},
                         MetricsDirection::read, MetricsVisibility::external_);
    TENZIR_ASSERT(message_sender_);
    ctx.spawn_task(folly::coro::co_withExecutor(
      evb_, read_loop(*evb_, std::move(bind_address).unwrap(), *message_sender_,
                      std::move(bytes_read_counter))));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    auto message = co_await message_receiver_.recv();
    if (not message) {
      // The sender side can be dropped after `await_task()` was already
      // enqueued. We cannot safely read `done_` here, so park this stale await
      // until the executor observes `state() == done` and cancels it.
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return std::move(*message);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](DatagramBatch batch) -> Task<void> {
        for (auto& datagram : batch.datagrams) {
          auto hostname = Option<std::string>{};
          if (args_.resolve_hostnames) {
            auto reverse_dns = co_await reverse_dns_.resolve(datagram.peer_ip);
            if (reverse_dns->is_err()) {
              if (not peer_resolution_warning_emitted_) {
                diagnostic::warning("{}", reverse_dns->unwrap_err().error)
                  .note("failed to resolve peer hostname for {}",
                        datagram.peer_ip)
                  .note("set `resolve_hostnames=false` to disable hostname "
                        "resolution")
                  .primary(args_.endpoint)
                  .emit(ctx);
                peer_resolution_warning_emitted_ = true;
              }
            } else if (auto* resolved
                       = try_as<ReverseDnsResolved>(&reverse_dns->unwrap())) {
              hostname = resolved->hostname;
            }
          }
          auto bytes = as_bytes(datagram.payload);
          auto string = std::string_view{
            reinterpret_cast<char const*>(bytes.data()), bytes.size()};
          auto utf8_valid = true;
          if (not args_.binary) {
            utf8_valid = arrow::util::ValidateUTF8(string);
          }
          if (not utf8_valid) {
            diagnostic::warning("message is not valid UTF-8")
              .primary(args_.endpoint)
              .note("peer: {}", datagram.peer_ip)
              .hint("use `binary=true` to accept non-UTF8 data")
              .emit(ctx);
            continue;
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
        // No need to flush buffered events on errors as we are shutting down.
        switch (error.stage) {
          case ErrorStage::startup:
            diagnostic::error("failed to start UDP socket")
              .primary(args_.endpoint)
              .note("{}", error.detail)
              .emit(ctx);
            co_return;
          case ErrorStage::runtime:
            diagnostic::error("failed to receive data from socket")
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
    co_return;
  }

  auto state() -> OperatorState override {
    return message_sender_ ? OperatorState::normal : OperatorState::done;
  }

  auto snapshot(Serde&) -> void override {
    // UDP datagrams in flight are inherently lossy across restarts; nothing
    // worth serialising here.
  }

private:
  explicit AcceptUdp(AcceptUdpArgs args,
                     std::tuple<Sender<Message>, Receiver<Message>> ch)
    : args_{std::move(args)},
      builder_{make_output_type(args_)},
      message_sender_{std::move(std::get<0>(ch))},
      message_receiver_{std::move(std::get<1>(ch))} {
  }

  static auto make_output_type(AcceptUdpArgs const& args) -> type {
    auto peer_record_fields = std::vector<record_type::field_view>{
      {"ip", ip_type{}},
      {"port", int64_type{}},
    };
    if (args.resolve_hostnames) {
      peer_record_fields.push_back({"hostname", string_type{}});
    }
    return type{
      "tenzir.accept_udp",
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
      socket.setRcvBuf(socket_receive_buffer_size);
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
        auto message = Message{DatagramBatch{std::vector<Datagram>{}}};
        if (auto* error = try_as<Error>(&callback.pending.front())) {
          message = std::move(*error);
          callback.pending.pop_front();
        } else {
          auto datagrams = std::vector<Datagram>{};
          datagrams.reserve(
            std::min(datagram_batch_size, callback.pending.size()));
          while (not callback.pending.empty()
                 and datagrams.size() < datagram_batch_size) {
            auto* datagram = try_as<Datagram>(&callback.pending.front());
            if (not datagram) {
              break;
            }
            datagrams.push_back(std::move(*datagram));
            callback.pending.pop_front();
          }
          message = DatagramBatch{std::move(datagrams)};
        }
        co_await message_sender.send(std::move(message));
        if (callback.paused and not callback.done
            and callback.pending.size() <= pending_low_water) {
          callback.paused = false;
          socket.resumeRead(&callback);
        }
      }
      if (callback.done) {
        break;
      }
      co_await callback.notify.wait();
      if (not callback.pending.empty()
          and not try_as<Error>(&callback.pending.front())
          and callback.pending.size() < datagram_batch_size) {
        co_await folly::coro::sleep(datagram_coalesce_delay);
      }
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

  AcceptUdpArgs args_;
  series_builder builder_;
  ForwardDnsResolver forward_dns_;
  ReverseDnsResolver reverse_dns_;
  folly::Executor::KeepAlive<folly::EventBase> evb_;
  Option<Sender<Message>> message_sender_;
  mutable Receiver<Message> message_receiver_;
  Option<folly::CancellationSource> batch_flush_cancel_;
  uint64_t batch_generation_ = 0;
  bool peer_resolution_warning_emitted_ = false;
};

class AcceptUdpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.accept_udp";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptUdpArgs, AcceptUdp>{};
    auto endpoint_arg = d.positional("endpoint", &AcceptUdpArgs::endpoint);
    d.named("resolve_hostnames", &AcceptUdpArgs::resolve_hostnames);
    d.named("binary", &AcceptUdpArgs::binary);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_str, ctx.get(endpoint_arg));
      auto location
        = ctx.get_location(endpoint_arg).value_or(location::unknown);
      if (not parse_socket_address(endpoint_str.inner,
                                   SocketAddressKind::bind)) {
        diagnostic::error("failed to parse endpoint")
          .primary(location)
          .emit(ctx);
      }
      return {};
    });
    auto desc = d.without_optimize();
    desc.name = "accept_udp";
    return desc;
  }
};

} // namespace

} // namespace tenzir::plugins::accept_udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_udp::AcceptUdpPlugin)
