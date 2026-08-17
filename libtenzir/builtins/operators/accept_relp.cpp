//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/async/dns.hpp>
#include <tenzir/async/metrics.hpp>
#include <tenzir/async/scope.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/async/stream.hpp>
#include <tenzir/async/tls.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/result.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/socket.hpp>
#include <tenzir/tls_options.hpp>

#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/coro/Retry.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace tenzir::plugins::accept_relp {

namespace {

using namespace tenzir::si_literals;

constexpr auto listen_backlog = uint32_t{128};
constexpr auto default_max_connections = uint64_t{128};
constexpr auto default_max_message_size = uint64_t{128_Ki};
constexpr auto message_queue_event_capacity = uint32_t{1'024};
constexpr auto ingress_batch_size = uint32_t{16};
static_assert(message_queue_event_capacity % ingress_batch_size == 0);
constexpr auto message_queue_capacity
  = message_queue_event_capacity / ingress_batch_size;
constexpr auto event_batch_size = int64_t{1'024};
constexpr auto event_batch_timeout = std::chrono::milliseconds{10};
constexpr auto response_queue_capacity = uint32_t{1'024};
constexpr auto response_coalesce_delay = std::chrono::microseconds{100};
constexpr auto read_buffer_size = size_t{16_Ki};
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
constexpr auto tls_probe_timeout = std::chrono::seconds{5};
constexpr auto latest_relp_version = uint32_t{1};
// Current librelp releases still advertise the experimental version 0.
constexpr auto librelp_compat_version = uint32_t{0};
constexpr auto max_transaction_id = uint32_t{999'999'999};

struct AcceptRelpArgs {
  located<std::string> endpoint;
  Option<located<uint64_t>> max_message_size;
  Option<located<uint64_t>> max_connections;
  Option<located<data>> tls;
  bool resolve_hostnames = false;
  bool auto_detect_tls = false;

  auto get_max_message_size() const -> size_t {
    return detail::narrow<size_t>(max_message_size ? max_message_size->inner
                                                   : default_max_message_size);
  }

  auto get_max_connections() const -> size_t {
    return detail::narrow<size_t>(max_connections ? max_connections->inner
                                                  : default_max_connections);
  }
};

struct PeerInfo {
  ip address;
  int64_t port;
};

struct RelpFrame {
  uint32_t transaction_id;
  std::string command;
  std::string data;
};

struct RelpPerformanceStats {
  Atomic<uint64_t> frames = {};
  Atomic<uint64_t> message_batches = {};
  Atomic<uint64_t> messages = {};
  Atomic<uint64_t> responses = {};
  Atomic<uint64_t> response_writes = {};
  Atomic<uint64_t> message_queue_stalls = {};
  Atomic<uint64_t> response_queue_stalls = {};
};

struct RelpConnectionMetrics {
  RelpConnectionMetrics(folly::coro::Transport const& transport,
                        metric_handler handler)
    : handle{transport.getPeerAddress().describe()},
      handler{std::move(handler)} {
  }

  std::string handle;
  metric_handler handler;
  Atomic<uint64_t> reads = {};
  Atomic<uint64_t> writes = {};
  Atomic<uint64_t> bytes_read = {};
  Atomic<uint64_t> bytes_written = {};
  Atomic<bool> closed = false;

  auto record_read(size_t bytes) -> void {
    reads.fetch_add(1, std::memory_order_relaxed);
    bytes_read.fetch_add(bytes, std::memory_order_relaxed);
  }

  auto record_write(size_t bytes) -> void {
    writes.fetch_add(1, std::memory_order_relaxed);
    bytes_written.fetch_add(bytes, std::memory_order_relaxed);
  }

  auto emit() -> void {
    handler.emit({
      {"handle", handle},
      {"reads", reads.exchange(0, std::memory_order_relaxed)},
      {"writes", writes.exchange(0, std::memory_order_relaxed)},
      {"bytes_read", bytes_read.exchange(0, std::memory_order_relaxed)},
      {"bytes_written", bytes_written.exchange(0, std::memory_order_relaxed)},
    });
  }

  auto close() -> void {
    if (closed.exchange(true, std::memory_order_relaxed)) {
      return;
    }
    emit();
  }

  auto is_closed() const -> bool {
    return closed.load(std::memory_order_relaxed);
  }
};

auto tcp_metrics_type() -> type {
  return {
    "tenzir.metrics.tcp",
    record_type{
      {"handle", string_type{}},
      {"reads", uint64_type{}},
      {"writes", uint64_type{}},
      {"bytes_read", uint64_type{}},
      {"bytes_written", uint64_type{}},
    },
  };
}

auto emit_tcp_metrics(Arc<RelpConnectionMetrics> metrics) -> Task<void> {
  while (true) {
    co_await sleep_for(defaults::metrics_interval);
    if (metrics->is_closed()) {
      co_return;
    }
    metrics->emit();
  }
}

auto make_peer_info(folly::SocketAddress const& address) -> PeerInfo {
  auto storage = sockaddr_storage{};
  auto length = address.getAddress(&storage);
  TENZIR_ASSERT(length > 0);
  auto result = ip{};
  if (storage.ss_family == AF_INET) {
    auto sockaddr = sockaddr_in{};
    std::memcpy(&sockaddr, &storage, sizeof(sockaddr));
    auto error = convert(sockaddr, result);
    TENZIR_ASSERT(not error);
  } else {
    TENZIR_ASSERT(storage.ss_family == AF_INET6);
    auto sockaddr = sockaddr_in6{};
    std::memcpy(&sockaddr, &storage, sizeof(sockaddr));
    auto error = convert(sockaddr, result);
    TENZIR_ASSERT(not error);
  }
  return {
    .address = result,
    .port = int64_t{address.getPort()},
  };
}

class RelpReader {
public:
  RelpReader(folly::coro::Transport& transport, size_t max_message_size,
             MetricsCounter& bytes_read_counter,
             RelpConnectionMetrics& connection_metrics)
    : transport_{transport},
      max_message_size_{max_message_size},
      bytes_read_counter_{bytes_read_counter},
      connection_metrics_{connection_metrics} {
  }

  auto has_complete_frame() const -> bool {
    // Only continue a batch when the next read cannot block on transport I/O.
    auto input = std::string_view{
      reinterpret_cast<char const*>(buffer_.data() + buffer_begin_),
      buffer_end_ - buffer_begin_,
    };
    auto transaction_end = input.find(' ');
    if (transaction_end == std::string_view::npos) {
      return false;
    }
    auto command_end = input.find(' ', transaction_end + 1);
    if (command_end == std::string_view::npos) {
      return false;
    }
    auto length_begin = command_end + 1;
    auto length_end = input.find_first_of(" \n", length_begin);
    if (length_end == std::string_view::npos) {
      return false;
    }
    auto length
      = to<uint32_t>(input.substr(length_begin, length_end - length_begin));
    if (not length) {
      return false;
    }
    if (*length == 0) {
      return input[length_end] == '\n';
    }
    if (input[length_end] != ' ') {
      return false;
    }
    auto payload = input.substr(length_end + 1);
    return payload.size() > *length and payload[*length] == '\n';
  }

  auto read() -> Task<Result<Option<RelpFrame>, std::string>> {
    auto first = co_await read_octet();
    if (not first) {
      co_return Option<RelpFrame>{None{}};
    }
    if (*first < '0' or *first > '9') {
      co_return Err{std::string{"transaction number must start with a digit"}};
    }
    auto transaction_text = std::string(1, static_cast<char>(*first));
    auto delimiter = co_await read_token(transaction_text, 9, true);
    if (delimiter.is_err()) {
      co_return Err{std::move(delimiter).unwrap_err()};
    }
    if (delimiter.unwrap() != ' ') {
      co_return Err{std::string{"missing space after transaction number"}};
    }
    auto transaction_id = to<uint32_t>(transaction_text);
    if (not transaction_id or *transaction_id > max_transaction_id) {
      co_return Err{std::string{"invalid transaction number"}};
    }
    auto command = std::string{};
    delimiter = co_await read_token(command, 32, false);
    if (delimiter.is_err()) {
      co_return Err{std::move(delimiter).unwrap_err()};
    }
    if (command.empty() or delimiter.unwrap() != ' ') {
      co_return Err{std::string{"invalid RELP command"}};
    }
    auto length_text = std::string{};
    delimiter = co_await read_token(length_text, 9, true);
    if (delimiter.is_err()) {
      co_return Err{std::move(delimiter).unwrap_err()};
    }
    if (length_text.empty()) {
      co_return Err{std::string{"missing RELP data length"}};
    }
    auto data_length = to<uint32_t>(length_text);
    if (not data_length) {
      co_return Err{std::string{"invalid RELP data length"}};
    }
    if (*data_length > max_message_size_) {
      co_return Err{
        fmt::format("RELP frame exceeds `max_message_size` ({} > {})",
                    *data_length, max_message_size_)};
    }
    if ((*data_length == 0 and delimiter.unwrap() != '\n')
        or (*data_length > 0 and delimiter.unwrap() != ' ')) {
      co_return Err{std::string{"invalid delimiter after RELP data length"}};
    }
    auto payload = std::string(*data_length, '\0');
    if (*data_length > 0) {
      auto bytes = std::span{reinterpret_cast<std::byte*>(payload.data()),
                             payload.size()};
      if (not co_await read_exact(bytes)) {
        co_return Err{std::string{"connection closed inside RELP payload"}};
      }
      auto trailer = co_await read_octet();
      if (not trailer or *trailer != '\n') {
        co_return Err{std::string{"missing RELP frame trailer"}};
      }
    }
    co_return Option<RelpFrame>{RelpFrame{
      .transaction_id = *transaction_id,
      .command = std::move(command),
      .data = std::move(payload),
    }};
  }

private:
  auto refill() -> Task<bool> {
    TENZIR_ASSERT(buffer_begin_ == buffer_end_);
    buffer_begin_ = 0;
    buffer_end_ = co_await transport_.read(
      folly::MutableByteRange{buffer_.data(), buffer_.size()},
      std::chrono::milliseconds{0});
    if (buffer_end_ == 0) {
      co_return false;
    }
    record_read(buffer_end_);
    co_return true;
  }

  auto read_octet() -> Task<Option<unsigned char>> {
    if (buffer_begin_ == buffer_end_ and not co_await refill()) {
      co_return None{};
    }
    co_return buffer_[buffer_begin_++];
  }

  auto read_exact(std::span<std::byte> bytes) -> Task<bool> {
    while (not bytes.empty()) {
      if (buffer_begin_ < buffer_end_) {
        auto count = std::min(bytes.size(), buffer_end_ - buffer_begin_);
        std::memcpy(bytes.data(), buffer_.data() + buffer_begin_, count);
        buffer_begin_ += count;
        bytes = bytes.subspan(count);
        continue;
      }
      auto* data = reinterpret_cast<unsigned char*>(bytes.data());
      auto count
        = co_await transport_.read(folly::MutableByteRange{data, bytes.size()},
                                   std::chrono::milliseconds{0});
      if (count == 0) {
        co_return false;
      }
      record_read(count);
      bytes = bytes.subspan(count);
    }
    co_return true;
  }

  auto read_token(std::string& result, size_t max_size, bool digits_only)
    -> Task<Result<unsigned char, std::string>> {
    while (true) {
      auto octet = co_await read_octet();
      if (not octet) {
        co_return Err{std::string{"connection closed inside RELP header"}};
      }
      if (*octet == ' ' or *octet == '\n') {
        co_return *octet;
      }
      auto valid = digits_only ? *octet >= '0' and *octet <= '9'
                               : (*octet >= 'a' and *octet <= 'z')
                                   or (*octet >= 'A' and *octet <= 'Z');
      if (not valid) {
        co_return Err{std::string{"invalid character in RELP header"}};
      }
      if (result.size() >= max_size) {
        co_return Err{std::string{"RELP header field is too long"}};
      }
      result.push_back(static_cast<char>(*octet));
    }
  }

  auto record_read(size_t bytes) -> void {
    bytes_read_counter_.add(bytes);
    connection_metrics_.record_read(bytes);
  }

  folly::coro::Transport& transport_;
  size_t max_message_size_;
  MetricsCounter& bytes_read_counter_;
  RelpConnectionMetrics& connection_metrics_;
  std::array<unsigned char, read_buffer_size> buffer_ = {};
  size_t buffer_begin_ = 0;
  size_t buffer_end_ = 0;
};

struct OpenOffer {
  uint32_t version;
};

auto parse_open_offer(std::string_view payload)
  -> Result<OpenOffer, std::string> {
  auto version = Option<uint32_t>{None{}};
  auto supports_syslog = false;
  while (not payload.empty()) {
    auto newline = payload.find('\n');
    auto line = payload.substr(0, newline);
    payload = newline == std::string_view::npos ? std::string_view{}
                                                : payload.substr(newline + 1);
    auto equals = line.find('=');
    if (equals == std::string_view::npos or equals == 0) {
      return Err{std::string{"invalid RELP open offer"}};
    }
    auto name = line.substr(0, equals);
    auto values = line.substr(equals + 1);
    if (name == "relp_version") {
      auto selected = Option<uint32_t>{None{}};
      while (not values.empty()) {
        auto comma = values.find(',');
        auto offered = values.substr(0, comma);
        if (offered.empty()
            or not std::ranges::all_of(offered, detail::ascii_isdigit)) {
          return Err{std::string{"invalid RELP version offer"}};
        }
        auto parsed = to<uint32_t>(offered);
        auto supported = parsed
                         and (*parsed == latest_relp_version
                              or *parsed == librelp_compat_version);
        if (supported and (not selected or *parsed > *selected)) {
          selected = *parsed;
        }
        values = comma == std::string_view::npos ? std::string_view{}
                                                 : values.substr(comma + 1);
      }
      if (not selected) {
        return Err{std::string{"unsupported RELP version"}};
      }
      version = *selected;
    } else if (name == "commands") {
      while (not values.empty()) {
        auto comma = values.find(',');
        auto command = values.substr(0, comma);
        supports_syslog = supports_syslog or command == "syslog";
        values = comma == std::string_view::npos ? std::string_view{}
                                                 : values.substr(comma + 1);
      }
    }
  }
  if (not version) {
    return Err{std::string{"missing `relp_version` offer"}};
  }
  if (not supports_syslog) {
    return Err{std::string{"client does not offer `commands=syslog`"}};
  }
  return OpenOffer{.version = *version};
}

auto next_transaction_id(uint32_t transaction_id) -> uint32_t {
  TENZIR_ASSERT(transaction_id > 0 and transaction_id <= max_transaction_id);
  return transaction_id == max_transaction_id ? uint32_t{1}
                                              : transaction_id + 1;
}

struct SyslogMessage {
  std::string payload;
  uint32_t transaction_id;
};

struct SyslogBatch {
  std::vector<SyslogMessage> messages;
  PeerInfo peer;
  Option<std::string> hostname;
};

struct Flush {
  uint64_t generation;
};

struct AcceptLoopFinished {};

using Message = variant<AcceptLoopFinished, Flush, SyslogBatch>;
using MessageQueue = BoundedQueue<Message>;

struct ResponseFrame {
  uint32_t first_transaction_id;
  uint32_t count;
  uint16_t status;
  std::string message;
};

struct ResponseWriterFinished {};

using Response = variant<ResponseFrame, ResponseWriterFinished>;
using ResponseQueue = BoundedQueue<Response>;

class AcceptRelp final : public Operator<void, table_slice> {
public:
  explicit AcceptRelp(AcceptRelpArgs args)
    : args_{std::move(args)},
      builder_{make_output_type(args_)},
      reverse_dns_{std::in_place,
                   ReverseDnsConfig{
                     .max_in_flight = args_.get_max_connections(),
                   }},
      connection_slots_{args_.get_max_connections()} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto bind_endpoint = Endpoint{};
    auto parsed = parsers::endpoint(args_.endpoint.inner, bind_endpoint)
                  and bind_endpoint.port;
    TENZIR_ASSERT(parsed);
    auto bind_address
      = co_await forward_dns_.resolve_bind_address(std::move(bind_endpoint));
    if (bind_address.is_err()) {
      diagnostic::error("failed to resolve listen address")
        .primary(args_.endpoint)
        .note("reason: {}", std::move(bind_address).unwrap_err())
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto tls = tls_options::from_optional(args_.tls, {.tls_default = false,
                                                      .is_server = true});
    auto resolved_tls = tls.resolve(ctx.actor_system().config(), ctx);
    if (not resolved_tls) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (args_.auto_detect_tls and not resolved_tls->tls.inner) {
      diagnostic::error("`auto_detect_tls` requires TLS to be enabled")
        .primary(args_.endpoint)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (resolved_tls->tls.inner) {
      auto context = resolved_tls->make_folly_ssl_context(ctx);
      if (not context) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      tls_context_ = std::move(*context);
    }
    io_executor_ = ctx.io_executor();
    evb_ = io_executor_->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    server_ = Box<folly::coro::ServerSocket>{std::in_place, std::move(socket),
                                             std::move(bind_address).unwrap(),
                                             listen_backlog};
    tcp_metrics_ = make_metric_handler(ctx, tcp_metrics_type());
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_relp"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    lifecycle_ = Lifecycle::running;
    ctx.spawn_task([this, &ctx]() -> Task<void> {
      auto notify_finished = detail::scope_guard{[this]() noexcept {
        message_queue_->force_enqueue(AcceptLoopFinished{});
      }};
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token,
        cancel_->getToken());
      co_await folly::coro::co_withCancellation(
        token, folly::coro::co_withExecutor(io_executor_, accept_loop(ctx)));
    });
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](SyslogBatch batch) -> Task<void> {
        for (auto& msg : batch.messages) {
          if (not detail::is_valid_utf8(msg.payload)) {
            diagnostic::warning(
              "dropped RELP syslog payload with invalid UTF-8")
              .primary(args_.endpoint)
              .note("peer: {}", batch.peer.address)
              .emit(ctx);
            continue;
          }
          auto event = builder_.record();
          event.field("data").data(std::string_view{msg.payload});
          auto peer = event.field("peer").record();
          peer.field("ip").data(batch.peer.address);
          peer.field("port").data(batch.peer.port);
          if (args_.resolve_hostnames and batch.hostname) {
            peer.field("hostname").data(std::string_view{*batch.hostname});
          }
          auto relp = event.field("relp").record();
          relp.field("transaction_id").data(int64_t{msg.transaction_id});
          if (builder_.length() == 1) {
            schedule_batch_flush(ctx);
          }
          if (builder_.length() >= event_batch_size) {
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
      [&](AcceptLoopFinished) -> Task<void> {
        cancel_batch_flush();
        co_await flush_builder(push);
        lifecycle_ = Lifecycle::done;
      });
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    begin_draining();
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    begin_draining();
    co_return FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  enum class Lifecycle {
    starting,
    running,
    draining,
    done,
  };

  static auto make_output_type(AcceptRelpArgs const& args) -> type {
    auto peer_fields = std::vector<record_type::field_view>{
      {"ip", ip_type{}},
      {"port", int64_type{}},
    };
    if (args.resolve_hostnames) {
      peer_fields.push_back({"hostname", string_type{}});
    }
    return type{
      "tenzir.accept_relp",
      record_type{
        {"data", string_type{}},
        {"peer", record_type{peer_fields}},
        {"relp", record_type{{"transaction_id", int64_type{}}}},
      },
    };
  }

  auto begin_draining() -> void {
    if (lifecycle_ == Lifecycle::draining or lifecycle_ == Lifecycle::done) {
      return;
    }
    lifecycle_ = Lifecycle::draining;
    cancel_batch_flush();
    cancel_->requestCancellation();
    if (server_ and evb_) {
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
        if (server_) {
          (*server_)->close();
        }
      });
    }
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(server_);
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      while (true) {
        auto slot = co_await connection_slots_.acquire();
        auto transport = co_await folly::coro::retryWithExponentialBackoff(
          std::numeric_limits<uint32_t>::max(), accept_retry_delay,
          accept_retry_delay, 0.0,
          [this, &ctx]() -> Task<std::unique_ptr<folly::coro::Transport>> {
            try {
              co_return co_await (*server_)->accept();
            } catch (folly::AsyncSocketException const& ex) {
              diagnostic::warning("failed to accept incoming RELP connection")
                .primary(args_.endpoint)
                .note("reason: {}", describe_socket_error(ex))
                .emit(ctx);
              throw;
            }
          },
          should_retry_socket);
        auto client
          = Box<folly::coro::Transport>::from_non_null(std::move(transport));
        scope.spawn(run_connection(std::move(client), std::move(slot), ctx));
      }
    });
  }

  auto run_connection(Box<folly::coro::Transport> transport,
                      SemaphorePermit slot, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(slot);
    try {
      co_await connection_loop(std::move(transport), ctx);
    } catch (folly::AsyncSocketException const& ex) {
      diagnostic::warning("RELP connection closed after I/O error")
        .primary(args_.endpoint)
        .note("reason: {}", describe_socket_error(ex))
        .emit(ctx);
    }
  }

  auto connection_loop(Box<folly::coro::Transport> transport, OpCtx& ctx)
    -> Task<void> {
    auto metrics = Arc<RelpConnectionMetrics>{
      std::in_place,
      *transport,
      tcp_metrics_,
    };
    auto close_metrics = detail::scope_guard{[metrics]() mutable noexcept {
      metrics->close();
    }};
    ctx.spawn_task(emit_tcp_metrics(metrics));
    auto peer = make_peer_info(transport->getPeerAddress());
    if (tls_context_) {
      try {
        auto should_upgrade = true;
        if (args_.auto_detect_tls) {
          should_upgrade
            = co_await probe_tls_client_hello(*transport, tls_probe_timeout);
        }
        if (should_upgrade) {
          transport = Box<folly::coro::Transport>{
            co_await upgrade_transport_to_tls_server(std::move(*transport),
                                                     tls_context_)};
        }
      } catch (folly::AsyncSocketException const& ex) {
        diagnostic::warning("TLS handshake failed")
          .primary(args_.endpoint)
          .note("peer IP: {}", peer.address)
          .note("reason: {}", describe_socket_error(ex))
          .hint("verify TLS settings and certificates on both sides")
          .emit(ctx);
        co_return;
      }
    }
    auto hostname = Option<std::string>{None{}};
    if (args_.resolve_hostnames) {
      auto result = co_await reverse_dns_->resolve(peer.address);
      if (result->is_err()) {
        diagnostic::warning("{}", result->unwrap_err().error)
          .note("failed to resolve peer hostname for {}", peer.address)
          .note("set `resolve_hostnames=false` to disable hostname resolution")
          .primary(args_.endpoint)
          .emit(ctx);
      } else if (auto* resolved
                 = try_as<ReverseDnsResolved>(&result->unwrap())) {
        hostname = resolved->hostname;
      }
    }
    auto bytes_read_counter = ctx.make_counter(
      MetricsLabel{"peer_ip", MetricsLabel::FixedString::truncate(
                                fmt::to_string(peer.address))},
      MetricsDirection::read, MetricsVisibility::external_, MetricsUnit::bytes);
    auto reader = RelpReader{*transport, args_.get_max_message_size(),
                             bytes_read_counter, *metrics};
    auto responses = Arc<ResponseQueue>{std::in_place, response_queue_capacity};
    auto performance = RelpPerformanceStats{};
    auto log_performance = detail::scope_guard{[&]() noexcept {
      TENZIR_DEBUG(
        "RELP connection stats: frames={}, message_batches={}, messages={}, "
        "responses={}, response_writes={}, message_queue_stalls={}, "
        "response_queue_stalls={}",
        performance.frames.load(std::memory_order_relaxed),
        performance.message_batches.load(std::memory_order_relaxed),
        performance.messages.load(std::memory_order_relaxed),
        performance.responses.load(std::memory_order_relaxed),
        performance.response_writes.load(std::memory_order_relaxed),
        performance.message_queue_stalls.load(std::memory_order_relaxed),
        performance.response_queue_stalls.load(std::memory_order_relaxed));
    }};
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      scope.spawn(
        write_responses(*transport, *metrics, responses, performance));
      auto finish_writer = detail::scope_guard{[&]() noexcept {
        responses->force_enqueue(ResponseWriterFinished{});
      }};
      co_await read_frames(reader, responses, peer, hostname, performance, ctx);
    });
  }

  auto read_frames(RelpReader& reader, Arc<ResponseQueue> responses,
                   PeerInfo const& peer, Option<std::string> const& hostname,
                   RelpPerformanceStats& performance, OpCtx& ctx)
    -> Task<void> {
    auto expected_transaction_id = uint32_t{1};
    auto opened = false;
    auto pending = std::vector<SyslogMessage>{};
    pending.reserve(ingress_batch_size);
    auto flush_pending = [&]() -> Task<void> {
      if (pending.empty()) {
        co_return;
      }
      auto first_transaction_id = pending.front().transaction_id;
      auto count = detail::narrow<uint32_t>(pending.size());
      auto batch = SyslogBatch{
        .messages = std::move(pending),
        .peer = peer,
        .hostname = hostname,
      };
      pending = std::vector<SyslogMessage>{};
      pending.reserve(ingress_batch_size);
      auto stalled = false;
      if (not message_queue_->try_enqueue(std::move(batch))) {
        stalled = true;
        co_await message_queue_->enqueue(std::move(batch));
      }
      performance.frames.fetch_add(count, std::memory_order_relaxed);
      performance.message_batches.fetch_add(1, std::memory_order_relaxed);
      performance.messages.fetch_add(count, std::memory_order_relaxed);
      performance.message_queue_stalls.fetch_add(static_cast<uint64_t>(stalled),
                                                 std::memory_order_relaxed);
      // A successful enqueue transfers ownership into the bounded in-memory
      // operator input. It does not imply durable downstream processing.
      co_await enqueue_response(responses, performance, first_transaction_id,
                                count, 200, "OK");
    };
    while (true) {
      auto frame_result = Result<Option<RelpFrame>, std::string>{};
      auto socket_error = Option<std::string>{None{}};
      try {
        frame_result = co_await reader.read();
      } catch (folly::AsyncSocketException const& ex) {
        socket_error = describe_socket_error(ex);
      }
      if (socket_error) {
        co_await flush_pending();
        diagnostic::warning("RELP connection closed after I/O error")
          .primary(args_.endpoint)
          .note("peer: {}", peer.address)
          .note("reason: {}", *socket_error)
          .emit(ctx);
        co_return;
      }
      if (frame_result.is_err()) {
        co_await flush_pending();
        diagnostic::warning("rejected malformed RELP frame")
          .primary(args_.endpoint)
          .note("peer: {}", peer.address)
          .note("reason: {}", std::move(frame_result).unwrap_err())
          .emit(ctx);
        co_return;
      }
      auto frame = std::move(frame_result).unwrap();
      if (not frame) {
        co_await flush_pending();
        co_return;
      }
      if (frame->transaction_id == 0
          or frame->transaction_id != expected_transaction_id) {
        co_await flush_pending();
        diagnostic::warning("rejected unexpected RELP transaction number")
          .primary(args_.endpoint)
          .note("peer: {}", peer.address)
          .note("expected: {}, got: {}", expected_transaction_id,
                frame->transaction_id)
          .emit(ctx);
        co_return;
      }
      expected_transaction_id = next_transaction_id(expected_transaction_id);
      if (frame->command == "syslog") {
        if (not opened) {
          co_await enqueue_response(responses, performance,
                                    frame->transaction_id, 1, 500,
                                    "session is not open");
          co_return;
        }
        pending.push_back(SyslogMessage{
          .payload = std::move(frame->data),
          .transaction_id = frame->transaction_id,
        });
        if (pending.size() >= ingress_batch_size
            or not reader.has_complete_frame()) {
          co_await flush_pending();
        }
        continue;
      }
      co_await flush_pending();
      performance.frames.fetch_add(1, std::memory_order_relaxed);
      if (frame->command == "open") {
        if (opened) {
          co_await enqueue_response(responses, performance,
                                    frame->transaction_id, 1, 500,
                                    "protocol error: connection already open");
          co_return;
        }
        auto offer = parse_open_offer(frame->data);
        if (offer.is_err()) {
          co_await enqueue_response(responses, performance,
                                    frame->transaction_id, 1, 500,
                                    std::move(offer).unwrap_err());
          co_return;
        }
        auto payload = fmt::format("OK\nrelp_version={}\ncommands=syslog",
                                   offer.unwrap().version);
        co_await enqueue_response(responses, performance, frame->transaction_id,
                                  1, 200, payload);
        opened = true;
        continue;
      }
      if (frame->command == "close") {
        if (not opened) {
          co_await enqueue_response(responses, performance,
                                    frame->transaction_id, 1, 500,
                                    "session is not open");
          co_return;
        }
        co_await enqueue_response(responses, performance, frame->transaction_id,
                                  1, 200, "OK");
        co_return;
      }
      if (frame->command == "rsp") {
        diagnostic::warning("received unexpected RELP response from client")
          .primary(args_.endpoint)
          .note("peer: {}", peer.address)
          .emit(ctx);
        co_return;
      }
      co_await enqueue_response(responses, performance, frame->transaction_id,
                                1, 500, "unsupported command");
      co_return;
    }
  }

  static auto flush_batch_after(Arc<MessageQueue> message_queue,
                                uint64_t generation) -> Task<void> {
    co_await sleep_for(event_batch_timeout);
    message_queue->force_enqueue(Flush{generation});
  }

  auto schedule_batch_flush(OpCtx& ctx) -> void {
    cancel_batch_flush();
    batch_generation_ += 1;
    batch_flush_cancel_.emplace();
    auto token = batch_flush_cancel_->getToken();
    auto generation = batch_generation_;
    ctx.spawn_task(folly::coro::co_withCancellation(
      token, folly::coro::co_withExecutor(
               io_executor_, flush_batch_after(message_queue_, generation))));
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
    auto slice = builder_.finish_assert_one_slice();
    auto rows = slice.rows();
    co_await push(std::move(slice));
    events_read_counter_.add(rows);
  }

  static auto write_frame(folly::coro::Transport& transport,
                          RelpConnectionMetrics& metrics, std::string frame)
    -> Task<void> {
    auto bytes = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(frame.data()), frame.size()};
    co_await transport.write(bytes);
    metrics.record_write(bytes.size());
  }

  static auto
  append_responses(std::string& output, ResponseFrame const& response) -> void {
    auto transaction_id = response.first_transaction_id;
    auto payload_size
      = fmt::formatted_size("{} {}", response.status, response.message);
    for (auto index = uint32_t{0}; index < response.count; ++index) {
      fmt::format_to(std::back_inserter(output), "{} rsp {} {} {}\n",
                     transaction_id, payload_size, response.status,
                     response.message);
      transaction_id = next_transaction_id(transaction_id);
    }
  }

  static auto
  write_responses(folly::coro::Transport& transport,
                  RelpConnectionMetrics& metrics, Arc<ResponseQueue> responses,
                  RelpPerformanceStats& performance) -> Task<void> {
    while (true) {
      auto response = co_await responses->dequeue();
      auto* first = try_as<ResponseFrame>(&response);
      if (not first) {
        co_return;
      }
      auto frames = std::string{};
      auto response_count = uint64_t{first->count};
      append_responses(frames, *first);
      co_await sleep_for(response_coalesce_delay);
      auto finished = false;
      while (auto next = responses->try_dequeue()) {
        if (auto* frame = try_as<ResponseFrame>(&*next)) {
          response_count += frame->count;
          append_responses(frames, *frame);
        } else {
          finished = true;
          break;
        }
      }
      co_await write_frame(transport, metrics, std::move(frames));
      performance.responses.fetch_add(response_count,
                                      std::memory_order_relaxed);
      performance.response_writes.fetch_add(1, std::memory_order_relaxed);
      if (finished) {
        co_return;
      }
    }
  }

  static auto enqueue_response(Arc<ResponseQueue> responses,
                               RelpPerformanceStats& performance,
                               uint32_t first_transaction_id, uint32_t count,
                               uint16_t status, std::string_view message)
    -> Task<void> {
    auto response = ResponseFrame{
      .first_transaction_id = first_transaction_id,
      .count = count,
      .status = status,
      .message = std::string{message},
    };
    if (not responses->try_enqueue(std::move(response))) {
      performance.response_queue_stalls.fetch_add(1, std::memory_order_relaxed);
      co_await responses->enqueue(std::move(response));
    }
  }

  AcceptRelpArgs args_;
  series_builder builder_;
  ForwardDnsResolver forward_dns_;
  Arc<ReverseDnsResolver> reverse_dns_;
  Semaphore connection_slots_;
  Option<folly::CancellationSource> batch_flush_cancel_;
  Box<folly::CancellationSource> cancel_{std::in_place};
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  folly::EventBase* evb_ = nullptr;
  Option<Box<folly::coro::ServerSocket>> server_;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  metric_handler tcp_metrics_ = {};
  MetricsCounter events_read_counter_;
  uint64_t batch_generation_ = 0;
  Lifecycle lifecycle_ = Lifecycle::starting;
};

class AcceptRelpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.accept_relp";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptRelpArgs, AcceptRelp>{};
    auto endpoint_arg = d.positional("endpoint", &AcceptRelpArgs::endpoint);
    auto max_message_size_arg
      = d.named("max_message_size", &AcceptRelpArgs::max_message_size);
    auto max_connections_arg
      = d.named("max_connections", &AcceptRelpArgs::max_connections);
    d.named("resolve_hostnames", &AcceptRelpArgs::resolve_hostnames);
    auto auto_detect_tls_arg
      = d.named("auto_detect_tls", &AcceptRelpArgs::auto_detect_tls);
    auto tls_arg = d.named("tls", &AcceptRelpArgs::tls, "record");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_text, ctx.get(endpoint_arg));
      auto endpoint = Endpoint{};
      auto location
        = ctx.get_location(endpoint_arg).value_or(location::unknown);
      if (not parsers::endpoint(endpoint_text.inner, endpoint)
          or not endpoint.port) {
        diagnostic::error("failed to parse endpoint")
          .primary(location)
          .emit(ctx);
      } else if (endpoint.port->type() != port_type::unknown
                 and endpoint.port->type() != port_type::tcp) {
        diagnostic::error("expected a TCP endpoint").primary(location).emit(ctx);
      }
      if (auto size = ctx.get(max_message_size_arg)) {
        if (size->inner == 0) {
          diagnostic::error("`max_message_size` must be greater than 0")
            .primary(size->source)
            .emit(ctx);
        } else if (size->inner > max_transaction_id) {
          diagnostic::error("`max_message_size` is too large")
            .primary(size->source)
            .note("maximum supported value: {}", max_transaction_id)
            .emit(ctx);
        }
      }
      if (auto count = ctx.get(max_connections_arg)) {
        if (count->inner == 0) {
          diagnostic::error("`max_connections` must be greater than 0")
            .primary(count->source)
            .emit(ctx);
        } else if (count->inner > static_cast<uint64_t>(
                     std::numeric_limits<int64_t>::max())) {
          diagnostic::error("`max_connections` is too large")
            .primary(count->source)
            .note("maximum supported value: {}",
                  std::numeric_limits<int64_t>::max())
            .emit(ctx);
        }
      }
      auto tls_enabled = Option<bool>{None{}};
      if (auto tls = ctx.get(tls_arg)) {
        auto options
          = tls_options{*tls, {.tls_default = false, .is_server = true}};
        if (not options.validate(ctx)) {
          return {};
        }
        tls_enabled = options.get_tls().inner;
      }
      if (auto auto_detect = ctx.get(auto_detect_tls_arg);
          auto_detect and *auto_detect and tls_enabled and not *tls_enabled) {
        diagnostic::error("`auto_detect_tls` requires TLS to be enabled")
          .primary(
            ctx.get_location(auto_detect_tls_arg).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    auto result = d.without_optimize();
    result.name = "accept_relp";
    return result;
  }
};

} // namespace

} // namespace tenzir::plugins::accept_relp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_relp::AcceptRelpPlugin)
