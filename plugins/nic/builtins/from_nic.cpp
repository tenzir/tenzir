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
#include <tenzir/plugin.hpp>
#include <tenzir/session.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/parser.hpp>

#include <folly/coro/BoundedQueue.h>
#include <folly/executors/GlobalExecutor.h>
#include <pcap/pcap.h>

#include <array>
#include <chrono>
#include <memory>
#include <string>

namespace tenzir::plugins::nic {

namespace {

constexpr auto queue_capacity = uint32_t{16};

using ChunkQueue = folly::coro::BoundedQueue<chunk_ptr>;

struct FromNicArgs {
  located<std::string> iface;
  Option<located<uint64_t>> snaplen;
  Option<located<std::string>> filter;
  Option<located<ir::pipeline>> parser;
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
    co_await ctx.spawn_sub<chunk_ptr>(caf::none, std::move(parser));
    auto snaplen
      = args_.snaplen ? args_.snaplen->inner : uint64_t{pcap::maximum_snaplen};
    ctx.spawn_task(folly::coro::co_withExecutor(
      folly::getGlobalCPUExecutor(),
      capture_loop(chunk_queue_, args_.iface.inner,
                   detail::narrow_cast<uint32_t>(snaplen), args_.filter,
                   ctx.dh())));
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
    auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
    auto push_result = co_await pipeline.push(std::move(chunk));
    if (push_result.is_err()) {
      done_ = true;
    }
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    sub_finished_->notify_one();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  static auto
  capture_loop(std::shared_ptr<ChunkQueue> queue, std::string iface,
               uint32_t snaplen, Option<located<std::string>> filter,
               diagnostic_handler& dh) -> Task<void> {
    auto finish = [&](auto&& emit) -> Task<void> {
      emit();
      co_await queue->enqueue(chunk_ptr{});
    };
    TENZIR_DEBUG("capturing from {} with snaplen of {}", iface, snaplen);
    auto packet_buffer_timeout_ms
      = std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(
          defaults::import::read_timeout)
          .count();
    auto error = std::array<char, PCAP_ERRBUF_SIZE>{};
    auto* raw = pcap_open_live(iface.c_str(), detail::narrow_cast<int>(snaplen),
                               1, packet_buffer_timeout_ms, error.data());
    if (not raw) {
      co_await finish([&] {
        diagnostic::error("failed to open interface: {}",
                          std::string_view{error.data()})
          .note("from `nic`")
          .emit(dh);
      });
      co_return;
    }
    auto pcap
      = std::unique_ptr<pcap_t, decltype(&pcap_close)>{raw, &pcap_close};
    if (filter) {
      TENZIR_DEBUG("applying capture filter `{}` on {}", filter->inner, iface);
      auto program = bpf_program{};
      auto netmask = lookup_capture_netmask(iface);
      if (pcap_compile(pcap.get(), &program, filter->inner.c_str(), 1, netmask)
          < 0) {
        auto err = std::string_view{::pcap_geterr(pcap.get())};
        co_await finish([&] {
          diagnostic::error("failed to compile capture filter: {}", err)
            .primary(filter->source)
            .note("capture filter: {}", filter->inner)
            .note("from `nic`")
            .emit(dh);
        });
        co_return;
      }
      auto set_filter_result = pcap_setfilter(pcap.get(), &program);
      pcap_freecode(&program);
      if (set_filter_result < 0) {
        auto err = std::string_view{::pcap_geterr(pcap.get())};
        co_await finish([&] {
          diagnostic::error("failed to install capture filter: {}", err)
            .primary(filter->source)
            .note("capture filter: {}", filter->inner)
            .note("from `nic`")
            .emit(dh);
        });
        co_return;
      }
    }
    auto linktype = pcap_datalink(pcap.get());
    TENZIR_ASSERT(linktype != PCAP_ERROR_NOT_ACTIVATED);
    auto metadata = chunk_metadata{
      .content_type = std::string{pcap::content_type},
    };
    auto buffer = std::vector<std::byte>{};
    auto num_buffered_packets = size_t{0};
    auto header_written = false;
    auto last_flush = std::chrono::steady_clock::now();
    auto flush = [&]() -> Task<void> {
      if (num_buffered_packets == 0) {
        co_return;
      }
      last_flush = std::chrono::steady_clock::now();
      co_await queue->enqueue(chunk::make(std::move(buffer), metadata));
      buffer.clear();
      num_buffered_packets = 0;
    };
    while (true) {
      const auto now = std::chrono::steady_clock::now();
      if (num_buffered_packets > 0
          and last_flush + defaults::import::batch_timeout < now) {
        co_await flush();
      }
      const u_char* pkt_data = nullptr;
      pcap_pkthdr* pkt_hdr = nullptr;
      auto result = ::pcap_next_ex(pcap.get(), &pkt_hdr, &pkt_data);
      if (result == 0) {
        continue;
      }
      if (result == -2) {
        break;
      }
      if (result == PCAP_ERROR) {
        auto err = std::string_view{::pcap_geterr(pcap.get())};
        co_await finish([&] {
          diagnostic::error("failed to get next packet: {}", err)
            .note("from `nic`")
            .emit(dh);
        });
        co_return;
      }
      if (not header_written) {
        auto header = make_capture_file_header(
          detail::narrow_cast<int>(snaplen), linktype);
        auto bytes = as_bytes(header);
        buffer.insert(buffer.end(), bytes.begin(), bytes.end());
        header_written = true;
      }
      auto header = pcap::packet_header{
        .timestamp = detail::narrow_cast<uint32_t>(pkt_hdr->ts.tv_sec),
        .timestamp_fraction
        = detail::narrow_cast<uint32_t>(pkt_hdr->ts.tv_usec),
        .captured_packet_length = pkt_hdr->caplen,
        .original_packet_length = pkt_hdr->len,
      };
      auto data = std::span<const std::byte>{
        reinterpret_cast<const std::byte*>(pkt_data),
        static_cast<size_t>(pkt_hdr->caplen),
      };
      auto header_bytes = as_bytes(header);
      buffer.reserve(buffer.size() + header_bytes.size() + data.size());
      buffer.insert(buffer.end(), header_bytes.begin(), header_bytes.end());
      buffer.insert(buffer.end(), data.begin(), data.end());
      ++num_buffered_packets;
      if (num_buffered_packets >= defaults::import::table_slice_size) {
        co_await flush();
      }
    }
    if (num_buffered_packets > 0) {
      co_await flush();
    }
    co_await queue->enqueue(chunk_ptr{});
  }

  FromNicArgs args_;
  bool done_ = false;
  bool capture_closed_ = false;
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
