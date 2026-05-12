//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/base_ctx.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/session.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/parser.hpp>

#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Task.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <pcap/pcap.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <memory>
#include <string>

namespace tenzir::plugins::nic {

namespace {

constexpr auto queue_capacity = uint32_t{16};
// Bound synchronous libpcap work per EventBase turn so one hot NIC does not
// monopolize the shared IO executor. If a dispatch saturates this limit, we
// reschedule ourselves immediately to continue draining.
constexpr auto dispatch_packet_limit = size_t{1024};

using ChunkQueue = folly::coro::BoundedQueue<chunk_ptr>;
using PcapHandle = std::unique_ptr<pcap_t, decltype(&pcap_close)>;

struct FromNicArgs {
  located<std::string> iface;
  Option<located<uint64_t>> snaplen;
  Option<located<std::string>> filter;
  Option<located<ir::pipeline>> parser;
};

struct CaptureSetup {
  explicit CaptureSetup(PcapHandle pcap) : pcap{std::move(pcap)} {
  }

  CaptureSetup(CaptureSetup&&) noexcept = default;
  auto operator=(CaptureSetup&&) noexcept -> CaptureSetup& = default;
  CaptureSetup(CaptureSetup const&) = delete;
  auto operator=(CaptureSetup const&) -> CaptureSetup& = delete;

  PcapHandle pcap;
  int linktype = 0;
  int selectable_fd = -1;
  std::chrono::milliseconds poll_interval = defaults::import::read_timeout;
};

struct DispatchState {
  chunk_ptr ready_chunk;
};

auto make_capture_file_header(int snaplen, int linktype) -> pcap::file_header {
  return {
    // Timestamps have microsecond resolution when using pcap_open_live(). If
    // we want nanosecond resolution, we must switch to pcap_create() and
    // pcap_activate().
    .magic_number = pcap::magic_number_1,
    .major_version = 2,
    .minor_version = 4,
    .reserved1 = 0,
    .reserved2 = 0,
    .snaplen = detail::narrow_cast<uint32_t>(snaplen),
    .linktype = detail::narrow_cast<uint32_t>(linktype),
  };
}

auto make_default_parser_pipeline(diagnostic_handler& dh)
  -> failure_or<ir::pipeline> {
  auto provider = session_provider::make(dh);
  auto session = provider.as_session();
  TRY(auto ast, parse("read_pcap", session));
  auto ctx = compile_ctx::make_root(base_ctx{session.dh(), session.reg()});
  return std::move(ast).compile(ctx);
}

auto lookup_capture_netmask(std::string const& iface) -> bpf_u_int32 {
  auto error = std::array<char, PCAP_ERRBUF_SIZE>{};
  auto network = bpf_u_int32{};
  auto netmask = bpf_u_int32{PCAP_NETMASK_UNKNOWN};
  if (pcap_lookupnet(iface.c_str(), &network, &netmask, error.data()) == -1) {
    return PCAP_NETMASK_UNKNOWN;
  }
  return netmask;
}

auto make_poll_interval(pcap_t& pcap) -> std::chrono::milliseconds {
  auto result = defaults::import::read_timeout;
  auto* required = ::pcap_get_required_select_timeout(std::addressof(pcap));
  if (required == nullptr) {
    return result;
  }
  auto required_timeout = std::chrono::seconds(required->tv_sec)
                          + std::chrono::microseconds(required->tv_usec);
  auto required_ms
    = std::chrono::duration_cast<std::chrono::milliseconds>(required_timeout);
  if (required_timeout > std::chrono::milliseconds::zero()
      and required_ms == std::chrono::milliseconds::zero()) {
    required_ms = std::chrono::milliseconds{1};
  }
  if (required_ms <= std::chrono::milliseconds::zero()) {
    return result;
  }
  return std::min(result, required_ms);
}

class CaptureChunkBuilder {
public:
  CaptureChunkBuilder(uint32_t snaplen, int linktype)
    : snaplen_{snaplen},
      linktype_{linktype},
      metadata_{.content_type = std::string{
                  pcap::content_type,
                }} {
  }

  auto append(const pcap_pkthdr& pkt_hdr, const u_char* pkt_data) -> chunk_ptr {
    if (not header_written_) {
      auto header = make_capture_file_header(detail::narrow_cast<int>(snaplen_),
                                             linktype_);
      auto bytes = as_bytes(header);
      buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
      header_written_ = true;
    }
    if (not batch_started_at_) {
      batch_started_at_ = std::chrono::steady_clock::now();
    }
    auto header = pcap::packet_header{
      .timestamp = detail::narrow_cast<uint32_t>(pkt_hdr.ts.tv_sec),
      .timestamp_fraction = detail::narrow_cast<uint32_t>(pkt_hdr.ts.tv_usec),
      .captured_packet_length = pkt_hdr.caplen,
      .original_packet_length = pkt_hdr.len,
    };
    auto data = std::span<const std::byte>{
      reinterpret_cast<const std::byte*>(pkt_data),
      static_cast<size_t>(pkt_hdr.caplen),
    };
    auto header_bytes = as_bytes(header);
    buffer_.reserve(buffer_.size() + header_bytes.size() + data.size());
    buffer_.insert(buffer_.end(), header_bytes.begin(), header_bytes.end());
    buffer_.insert(buffer_.end(), data.begin(), data.end());
    ++num_buffered_packets_;
    if (num_buffered_packets_ >= defaults::import::table_slice_size) {
      return flush();
    }
    return chunk_ptr{};
  }

  auto flush_if_ready(std::chrono::steady_clock::time_point now) -> chunk_ptr {
    // Match series_builder::yield_ready() semantics: once we buffer the first
    // packet of a batch, the timeout is measured from that oldest buffered
    // packet rather than being reset by later arrivals.
    if (not batch_started_at_
        or *batch_started_at_ + defaults::import::batch_timeout > now) {
      return chunk_ptr{};
    }
    return flush();
  }

  auto flush() -> chunk_ptr {
    if (num_buffered_packets_ == 0) {
      return chunk_ptr{};
    }
    auto result = chunk::make(std::move(buffer_), metadata_);
    buffer_.clear();
    num_buffered_packets_ = 0;
    batch_started_at_ = None{};
    return result;
  }

private:
  uint32_t snaplen_ = 0;
  int linktype_ = 0;
  chunk_metadata metadata_;
  std::vector<std::byte> buffer_;
  Option<std::chrono::steady_clock::time_point> batch_started_at_ = None{};
  size_t num_buffered_packets_ = 0;
  bool header_written_ = false;
};

class PcapWakeup final : private folly::EventHandler,
                         private folly::AsyncTimeout {
public:
  PcapWakeup(folly::EventBase& evb, std::chrono::milliseconds poll_interval,
             int selectable_fd)
    : folly::EventHandler{&evb},
      folly::AsyncTimeout{&evb},
      poll_interval_{poll_interval} {
    if (selectable_fd != -1) {
      initHandler(&evb, folly::NetworkSocket::fromFd(selectable_fd));
      registerHandler(EventHandler::READ | EventHandler::PERSIST);
    }
    scheduleTimeout(poll_interval_);
  }

  ~PcapWakeup() override {
    cancelTimeout();
    if (isHandlerRegistered()) {
      unregisterHandler();
    }
  }

  auto wait() -> Task<void> {
    co_await notify_.wait();
  }

  auto kick() -> void {
    notify_.notify_one();
  }

private:
  auto handlerReady(uint16_t events) noexcept -> void override {
    if ((events & EventHandler::READ) != 0) {
      notify_.notify_one();
    }
  }

  auto timeoutExpired() noexcept -> void override {
    notify_.notify_one();
    scheduleTimeout(poll_interval_);
  }

  std::chrono::milliseconds poll_interval_;
  Notify notify_;
};

auto make_capture_setup(std::string const& iface, uint32_t snaplen,
                        Option<located<std::string>> const& filter,
                        diagnostic_handler& dh) -> Option<CaptureSetup> {
  TENZIR_DEBUG("capturing from {} with snaplen of {}", iface, snaplen);
  auto packet_buffer_timeout_ms
    = std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(
        defaults::import::read_timeout)
        .count();
  auto error = std::array<char, PCAP_ERRBUF_SIZE>{};
  auto* raw = pcap_open_live(iface.c_str(), detail::narrow_cast<int>(snaplen),
                             1, packet_buffer_timeout_ms, error.data());
  if (not raw) {
    diagnostic::error("failed to open interface: {}",
                      std::string_view{error.data()})
      .note("from `nic`")
      .emit(dh);
    return None{};
  }
  auto result = CaptureSetup{PcapHandle{raw, &pcap_close}};
  if (filter) {
    TENZIR_DEBUG("applying capture filter `{}` on {}", filter->inner, iface);
    auto program = bpf_program{};
    auto netmask = lookup_capture_netmask(iface);
    if (pcap_compile(result.pcap.get(), &program, filter->inner.c_str(), 1,
                     netmask)
        < 0) {
      auto err = std::string_view{::pcap_geterr(result.pcap.get())};
      diagnostic::error("failed to compile capture filter: {}", err)
        .primary(filter->source)
        .note("capture filter: {}", filter->inner)
        .note("from `nic`")
        .emit(dh);
      return None{};
    }
    auto set_filter_result = pcap_setfilter(result.pcap.get(), &program);
    pcap_freecode(&program);
    if (set_filter_result < 0) {
      auto err = std::string_view{::pcap_geterr(result.pcap.get())};
      diagnostic::error("failed to install capture filter: {}", err)
        .primary(filter->source)
        .note("capture filter: {}", filter->inner)
        .note("from `nic`")
        .emit(dh);
      return None{};
    }
  }
  result.linktype = pcap_datalink(result.pcap.get());
  TENZIR_ASSERT(result.linktype != PCAP_ERROR_NOT_ACTIVATED);
  auto nonblock_error = std::array<char, PCAP_ERRBUF_SIZE>{};
  if (pcap_setnonblock(result.pcap.get(), 1, nonblock_error.data()) != 0) {
    diagnostic::error("failed to enable non-blocking capture: {}",
                      std::string_view{nonblock_error.data()})
      .note("from `nic`")
      .emit(dh);
    return None{};
  }
  result.selectable_fd = pcap_get_selectable_fd(result.pcap.get());
  result.poll_interval = make_poll_interval(*result.pcap);
  return result;
}

auto dispatch_packets(u_char* user, const pcap_pkthdr* pkt_hdr,
                      const u_char* pkt_data) -> void {
  TENZIR_ASSERT(user);
  TENZIR_ASSERT(pkt_hdr);
  TENZIR_ASSERT(pkt_data);
  auto& state
    = *reinterpret_cast<std::pair<CaptureChunkBuilder*, DispatchState*>*>(user);
  auto chunk = state.first->append(*pkt_hdr, pkt_data);
  if (chunk) {
    TENZIR_ASSERT(not state.second->ready_chunk);
    state.second->ready_chunk = std::move(chunk);
  }
}

class FromNic final : public Operator<void, table_slice> {
public:
  explicit FromNic(FromNicArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto parser = ir::pipeline{};
    if (args_.parser) {
      parser = args_.parser->inner;
    } else {
      auto default_parser = make_default_parser_pipeline(ctx.dh());
      if (not default_parser) {
        done_ = true;
        co_return;
      }
      parser = std::move(*default_parser);
    }
    if (not parser.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      done_ = true;
      co_return;
    }
    auto snaplen
      = args_.snaplen ? args_.snaplen->inner : uint64_t{pcap::maximum_snaplen};
    auto capture_snaplen = detail::narrow_cast<uint32_t>(snaplen);
    auto setup = make_capture_setup(args_.iface.inner, capture_snaplen,
                                    args_.filter, ctx.dh());
    if (not setup) {
      done_ = true;
      co_return;
    }
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_nic"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsType::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_nic"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsType::events);
    co_await ctx.spawn_sub<chunk_ptr>(caf::none, std::move(parser));
    auto io_executor = ctx.io_executor();
    auto* evb = io_executor->getEventBase();
    ctx.spawn_task(folly::coro::co_withExecutor(
      io_executor,
      capture_loop(chunk_queue_, args_.iface.inner, capture_snaplen,
                   std::move(*setup), *evb, ctx.dh())));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (capture_closed_) {
      co_await sub_finished_->wait();
      co_return chunk_ptr{};
    }
    co_return co_await chunk_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      done_ = true;
      co_return;
    }
    auto chunk = std::move(result).as<chunk_ptr>();
    if (not chunk) {
      if (capture_closed_) {
        done_ = true;
        co_return;
      }
      auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
      co_await pipeline.close();
      capture_closed_ = true;
      co_return;
    }
    const auto bytes = chunk->size();
    auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
    auto push_result = co_await pipeline.push(std::move(chunk));
    if (push_result.is_err()) {
      done_ = true;
    } else if (bytes > 0) {
      bytes_read_counter_.add(bytes);
    }
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    sub_finished_->notify_one();
    co_return;
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx&) -> Task<void> override {
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    events_read_counter_.add(rows);
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  static auto capture_loop(std::shared_ptr<ChunkQueue> queue, std::string iface,
                           uint32_t snaplen, CaptureSetup setup,
                           folly::EventBase& evb, diagnostic_handler& dh)
    -> Task<void> {
    TENZIR_DEBUG("capturing from {} with async pcap dispatch and snaplen of {}",
                 iface, snaplen);
    auto wakeup = PcapWakeup{evb, setup.poll_interval, setup.selectable_fd};
    auto builder = CaptureChunkBuilder{snaplen, setup.linktype};
    auto finish = [&](auto&& emit) -> Task<void> {
      if (auto chunk = builder.flush()) {
        co_await queue->enqueue(std::move(chunk));
      }
      emit();
      co_await queue->enqueue(chunk_ptr{});
    };
    while (true) {
      co_await wakeup.wait();
      auto now = std::chrono::steady_clock::now();
      if (auto chunk = builder.flush_if_ready(now)) {
        co_await queue->enqueue(std::move(chunk));
      }
      auto state = DispatchState{};
      auto callback_state = std::pair{&builder, &state};
      auto result = ::pcap_dispatch(
        setup.pcap.get(), detail::narrow_cast<int>(dispatch_packet_limit),
        &dispatch_packets, reinterpret_cast<u_char*>(&callback_state));
      if (state.ready_chunk) {
        co_await queue->enqueue(std::move(state.ready_chunk));
      }
      if (result == 0) {
        co_await folly::coro::co_safe_point;
        continue;
      }
      if (result == PCAP_ERROR_BREAK) {
        if (auto chunk = builder.flush()) {
          co_await queue->enqueue(std::move(chunk));
        }
        co_await queue->enqueue(chunk_ptr{});
        co_return;
      }
      if (result == PCAP_ERROR) {
        auto err = std::string_view{::pcap_geterr(setup.pcap.get())};
        co_await finish([&] {
          diagnostic::error("failed to dispatch packets: {}", err)
            .note("from `nic`")
            .emit(dh);
        });
        co_return;
      }
      TENZIR_ASSERT(result > 0);
      if (result == detail::narrow_cast<int>(dispatch_packet_limit)) {
        wakeup.kick();
        co_await folly::coro::co_reschedule_on_current_executor;
      } else {
        co_await folly::coro::co_safe_point;
      }
    }
  }

  FromNicArgs args_;
  bool done_ = false;
  bool capture_closed_ = false;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  mutable std::shared_ptr<Notify> sub_finished_ = std::make_shared<Notify>();
  mutable std::shared_ptr<ChunkQueue> chunk_queue_
    = std::make_shared<ChunkQueue>(queue_capacity);
};

class FromNicPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_nic";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromNicArgs, FromNic>{};
    auto iface = d.positional("iface", &FromNicArgs::iface);
    auto snaplen = d.named("snaplen", &FromNicArgs::snaplen);
    auto filter = d.named("filter", &FromNicArgs::filter);
    auto parser = d.pipeline(&FromNicArgs::parser);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto iface_value, ctx.get(iface));
      if (iface_value.inner.empty()) {
        diagnostic::error("interface must not be empty")
          .primary(iface_value.source)
          .emit(ctx);
      }
      if (auto snaplen_value = ctx.get(snaplen)) {
        if (snaplen_value->inner == 0) {
          diagnostic::error("`snaplen` must be greater than zero")
            .primary(snaplen_value->source)
            .emit(ctx);
        } else if (snaplen_value->inner > pcap::maximum_snaplen) {
          diagnostic::error("`snaplen` must be <= {}", pcap::maximum_snaplen)
            .primary(snaplen_value->source)
            .emit(ctx);
        }
      }
      if (auto filter_value = ctx.get(filter)) {
        if (filter_value->inner.empty()) {
          diagnostic::error("`filter` must not be empty")
            .primary(filter_value->source)
            .emit(ctx);
        }
      }
      if (auto parser_value = ctx.get(parser)) {
        auto output = parser_value->inner.infer_type(tag_v<chunk_ptr>, ctx);
        if (output.is_error()) {
          return {};
        }
        if (not *output or (*output)->is_not<table_slice>()) {
          diagnostic::error("pipeline must return events")
            .primary(parser_value->source.subloc(0, 1))
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::nic

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::FromNicPlugin)
