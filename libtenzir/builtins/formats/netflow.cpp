//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/netflow.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/pusher.hpp"
#include "tenzir/blob.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/view3.hpp"

#include <algorithm>
#include <limits>
#include <string_view>
#include <vector>

namespace tenzir::plugins::read_netflow {

namespace {

using netflow::DecodedRecord;
using netflow::Decoder;
using netflow::FrameResult;
using netflow::FrameStatus;
using netflow::IdleFrameTimer;
using netflow::Peer;
using netflow::RecordKind;
using netflow::Version;

using namespace si_literals;

constexpr auto max_stream_buffer_bytes = size_t{16_Mi};
// NetFlow v9 has no message length. Give another FlowSet one normal batching
// interval to arrive before accepting a record-count-compatible prefix.
constexpr auto ambiguous_stream_timeout = defaults::import::batch_timeout;

struct ReadNetflowArgs {
  location operator_location;
};

/// Stamps the operator location onto diagnostics that the decoder and the
/// builders emit without a source span.
auto make_dh(diagnostic_handler& dh, location operator_location)
  -> transforming_diagnostic_handler {
  return transforming_diagnostic_handler{
    dh, [operator_location](diagnostic d) {
      if (operator_location != location::unknown) {
        d.annotations.emplace_back(false, "", operator_location);
      }
      return d;
    }};
}

class BuilderSet {
public:
  auto start(diagnostic_handler& dh) -> void {
    v5_.emplace(options("netflow.v5"), dh);
    v9_.emplace(options("netflow.v9"), dh);
    ipfix_.emplace(options("netflow.ipfix"), dh);
  }

  auto append(std::vector<DecodedRecord> records)
    -> series_builder::YieldReadyResult {
    auto result = series_builder::YieldReadyResult{};
    for (auto& record : records) {
      if (active_ and *active_ != record.metadata.version) {
        flush_active(result);
      }
      active_ = record.metadata.version;
      append(record);
    }
    return result;
  }

  auto yield_ready(std::chrono::steady_clock::time_point now
                   = std::chrono::steady_clock::now())
    -> series_builder::YieldReadyResult {
    auto result = series_builder::YieldReadyResult{};
    if (active_) {
      result.merge(builder(*active_).yield_ready_as_table_slice(now));
    }
    return result;
  }

  auto finalize() -> std::vector<table_slice> {
    auto result = std::vector<table_slice>{};
    for (auto* builder : {&*v5_, &*v9_, &*ipfix_}) {
      auto slices = builder->finalize_as_table_slice();
      result.insert(result.end(), std::make_move_iterator(slices.begin()),
                    std::make_move_iterator(slices.end()));
    }
    active_ = None{};
    return result;
  }

private:
  static auto options(std::string name) -> multi_series_builder::options {
    auto result = multi_series_builder::options{};
    result.settings.default_schema_name = std::move(name);
    result.settings.ordered = true;
    return result;
  }

  auto builder(Version version) -> multi_series_builder& {
    switch (version) {
      case Version::v5:
        return *v5_;
      case Version::v9:
        return *v9_;
      case Version::ipfix:
        return *ipfix_;
    }
    TENZIR_UNREACHABLE();
  }

  auto flush_active(series_builder::YieldReadyResult& result) -> void {
    TENZIR_ASSERT(active_);
    auto slices = builder(*active_).finalize_as_table_slice();
    result.slices.insert(result.slices.end(),
                         std::make_move_iterator(slices.begin()),
                         std::make_move_iterator(slices.end()));
  }

  auto append(DecodedRecord const& record) -> void {
    auto event = builder(record.metadata.version).record();
    auto metadata = event.exact_field("netflow").record();
    metadata.exact_field("version").data(
      uint64_t{static_cast<uint16_t>(record.metadata.version)});
    metadata.exact_field("record_type")
      .data(record.metadata.record_kind == RecordKind::flow ? "flow"
                                                            : "options");
    metadata.exact_field("export_time").data(record.metadata.export_time);
    metadata.exact_field("sequence_number")
      .data(uint64_t{record.metadata.sequence_number});
    if (record.metadata.observation_domain_id) {
      metadata.exact_field("observation_domain_id")
        .data(uint64_t{*record.metadata.observation_domain_id});
    }
    if (record.metadata.template_id) {
      metadata.exact_field("template_id")
        .data(uint64_t{*record.metadata.template_id});
    }
    if (record.metadata.sys_uptime) {
      metadata.exact_field("sys_uptime").data(*record.metadata.sys_uptime);
    }
    if (record.metadata.exporter) {
      auto exporter = metadata.exact_field("exporter").record();
      exporter.exact_field("ip").data(record.metadata.exporter->address);
      exporter.exact_field("port").data(
        int64_t{record.metadata.exporter->port});
    }
    if (record.metadata.v5) {
      metadata.exact_field("engine_type")
        .data(uint64_t{record.metadata.v5->engine_type});
      metadata.exact_field("engine_id")
        .data(uint64_t{record.metadata.v5->engine_id});
      metadata.exact_field("sampling_mode")
        .data(uint64_t{record.metadata.v5->sampling_mode});
      metadata.exact_field("sampling_interval")
        .data(uint64_t{record.metadata.v5->sampling_interval});
    }
    for (auto const& field : record.fields) {
      event.exact_field(field.name).data(field.value);
    }
  }

  Option<multi_series_builder> v5_;
  Option<multi_series_builder> v9_;
  Option<multi_series_builder> ipfix_;
  Option<Version> active_;
};

auto find_field(record_view3 record, std::string_view name)
  -> Option<data_view3> {
  for (auto [field_name, value] : record) {
    if (field_name == name) {
      return value;
    }
  }
  return None{};
}

struct MessageEvent {
  blob_view data;
  Option<Peer> peer;
};

auto parse_message_event(record_view3 row) -> Option<MessageEvent> {
  auto data = find_field(row, "data");
  if (not data) {
    return None{};
  }
  auto const* payload = try_as<blob_view>(&*data);
  if (not payload) {
    return None{};
  }
  auto peer = find_field(row, "peer");
  if (not peer or is<caf::none_t>(*peer)) {
    return MessageEvent{.data = *payload, .peer = None{}};
  }
  auto const* peer_record = try_as<record_view3>(&*peer);
  if (not peer_record) {
    return None{};
  }
  auto address = find_field(*peer_record, "ip");
  auto port = find_field(*peer_record, "port");
  if (not address or not port) {
    return None{};
  }
  auto const* peer_ip = try_as<ip>(&*address);
  auto const* peer_port = try_as<int64_t>(&*port);
  if (not peer_ip or not peer_port or *peer_port < 0
      or *peer_port > std::numeric_limits<uint16_t>::max()) {
    return None{};
  }
  return MessageEvent{
    .data = *payload,
    .peer = Peer{*peer_ip, static_cast<uint16_t>(*peer_port)},
  };
}

auto emit_envelope_error(ReadNetflowArgs const& args, diagnostic_handler& dh)
  -> void {
  diagnostic::error("read_netflow expects binary message events")
    .primary(args.operator_location)
    .note("expected `{data: blob, peer?: {ip: ip, port: int64}}`")
    .hint("provide one complete NetFlow or IPFIX message in each `data` field")
    .emit(dh);
}

class ReadNetflowStream final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadNetflowStream(ReadNetflowArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    dh_.emplace(make_dh(ctx.dh(), args_.operator_location));
    builders_.start(*dh_);
    co_return;
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    auto bytes = as_bytes(input);
    ambiguous_frame_.on_input(bytes.size());
    TENZIR_ASSERT(buffer_.size() <= max_stream_buffer_bytes);
    if (bytes.size() > max_stream_buffer_bytes - buffer_.size()) {
      diagnostic::error("NetFlow byte stream exceeds the framing buffer limit")
        .primary(args_.operator_location)
        .note("retaining {} buffered bytes plus {} input bytes would exceed "
              "the {}-byte limit",
              buffer_.size(), bytes.size(), max_stream_buffer_bytes)
        .note("the input may contain a truncated NetFlow v9 message")
        .emit(ctx);
      failed_ = true;
      buffer_.clear();
      co_return;
    }
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
    co_await pusher_.push(
      with_ambiguous_frame_wakeup(process_available(false, ctx)), push);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto ready = series_builder::YieldReadyResult{};
    auto const now = std::chrono::steady_clock::now();
    if (auto frame_size = ambiguous_frame_.take_expired(now)) {
      ready = process_available(false, ctx, frame_size);
    } else {
      // Input can replace a dequeued timeout with a later deadline. Treat that
      // wakeup as stale and schedule the remaining delay below.
      ready = builders_.yield_ready(now);
    }
    co_await pusher_.push(with_ambiguous_frame_wakeup(std::move(ready), now),
                          push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    ambiguous_frame_.reset();
    if (not failed_) {
      co_await pusher_.push(process_available(true, ctx), push);
      decoder_.finish(*dh_);
    }
    for (auto& slice : builders_.finalize()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    for (auto& slice : builders_.finalize()) {
      co_await push(std::move(slice));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    decoder_.snapshot(serde);
    // The buffer holds an undecodable incomplete or ambiguous prefix that
    // cannot be flushed in `prepare_snapshot()`, so serialize it despite its
    // potential size; the overflow check in `process()` bounds it to
    // `max_stream_buffer_bytes`. The idle grace timer for an ambiguous prefix
    // is steady-clock state that cannot survive a restart; instead of
    // restoring it, the next input re-frames the buffer and re-arms the timer
    // through `process()`, and `finalize()` resolves the end of the stream.
    // Only a restored ambiguous prefix on a stream that never sends another
    // byte and never closes waits indefinitely; that delays delivery but
    // loses nothing.
    serde("netflow_stream_buffer", buffer_);
    serde("netflow_stream_failed", failed_);
  }

private:
  auto with_ambiguous_frame_wakeup(series_builder::YieldReadyResult result,
                                   std::chrono::steady_clock::time_point now
                                   = std::chrono::steady_clock::now()) const
    -> series_builder::YieldReadyResult {
    if (auto wait_for = ambiguous_frame_.wait_for(now)) {
      auto wakeup = series_builder::YieldReadyResult{};
      wakeup.wait_for = *wait_for;
      result.merge(std::move(wakeup));
    }
    return result;
  }

  auto process_available(bool end_of_input, OpCtx& ctx,
                         Option<size_t> committed_frame_size = None{})
    -> series_builder::YieldReadyResult {
    auto consumed = size_t{0};
    auto ready = series_builder::YieldReadyResult{};
    while (consumed < buffer_.size()) {
      auto remaining = std::span<const std::byte>{buffer_}.subspan(consumed);
      auto framed = FrameResult{};
      if (committed_frame_size) {
        TENZIR_ASSERT(*committed_frame_size <= remaining.size());
        framed = decoder_.frame(remaining.first(*committed_frame_size), true);
        committed_frame_size = None{};
      } else {
        framed = decoder_.frame(remaining, end_of_input);
      }
      if (framed.status == FrameStatus::incomplete) {
        ambiguous_frame_.reset();
        break;
      }
      if (framed.status == FrameStatus::ambiguous) {
        TENZIR_ASSERT(framed.size > 0 and framed.size <= buffer_.size());
        ambiguous_frame_.observe(framed.size);
        break;
      }
      if (framed.status == FrameStatus::error) {
        ambiguous_frame_.reset();
        diagnostic::error("failed to frame NetFlow byte stream")
          .primary(args_.operator_location)
          .note("{}", framed.message)
          .note("safe resynchronization is impossible for an unframed byte "
                "stream")
          .emit(ctx);
        failed_ = true;
        buffer_.clear();
        return ready;
      }
      auto result
        = decoder_.decode_message(remaining.first(framed.size), None{}, *dh_);
      if (result.error) {
        diagnostic::error("failed to decode NetFlow byte stream")
          .primary(args_.operator_location)
          .note("{}", result.error->message)
          .emit(ctx);
        failed_ = true;
        buffer_.clear();
        return ready;
      }
      ready.merge(builders_.append(std::move(result.records)));
      ready.merge(builders_.yield_ready());
      consumed += framed.size;
    }
    if (consumed > 0) {
      buffer_.erase(buffer_.begin(), buffer_.begin() + consumed);
    }
    return ready;
  }

  ReadNetflowArgs args_;
  Option<transforming_diagnostic_handler> dh_;
  Decoder decoder_;
  BuilderSet builders_;
  SeriesPusher pusher_;
  std::vector<std::byte> buffer_;
  IdleFrameTimer ambiguous_frame_{ambiguous_stream_timeout};
  bool failed_ = false;
};

class ReadNetflowEvents final : public Operator<table_slice, table_slice> {
public:
  explicit ReadNetflowEvents(ReadNetflowArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    dh_.emplace(make_dh(ctx.dh(), args_.operator_location));
    builders_.start(*dh_);
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto ready = series_builder::YieldReadyResult{};
    for (auto row : values3(input)) {
      auto message = parse_message_event(row);
      if (not message) {
        emit_envelope_error(args_, ctx.dh());
        co_return;
      }
      auto result = decoder_.decode_message(message->data, message->peer, *dh_);
      if (result.error) {
        if (result.error->kind
            == netflow::DecodeErrorKind::unsupported_version) {
          auto warning
            = diagnostic::warning("unsupported NetFlow message version")
                .primary(args_.operator_location);
          if (message->peer) {
            warning = std::move(warning).note(
              "peer: {}:{}", message->peer->address, message->peer->port);
          }
          std::move(warning).note("{}", result.error->message).emit(ctx);
        } else {
          auto warning = diagnostic::warning("malformed NetFlow message")
                           .primary(args_.operator_location);
          if (message->peer) {
            warning = std::move(warning).note(
              "peer: {}:{}", message->peer->address, message->peer->port);
          }
          std::move(warning).note("{}", result.error->message).emit(ctx);
        }
        continue;
      }
      ready.merge(builders_.append(std::move(result.records)));
    }
    ready.merge(builders_.yield_ready());
    co_await pusher_.push(std::move(ready), push);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    co_await pusher_.push(builders_.yield_ready(), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    decoder_.finish(*dh_);
    for (auto& slice : builders_.finalize()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    for (auto& slice : builders_.finalize()) {
      co_await push(std::move(slice));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    decoder_.snapshot(serde);
  }

private:
  ReadNetflowArgs args_;
  Option<transforming_diagnostic_handler> dh_;
  Decoder decoder_;
  BuilderSet builders_;
  SeriesPusher pusher_;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_netflow";
  }

  auto describe() const -> Description override {
    auto description
      = Describer<ReadNetflowArgs, ReadNetflowStream, ReadNetflowEvents>{};
    description.operator_location(&ReadNetflowArgs::operator_location);
    return description.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_netflow

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_netflow::Plugin)
