//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/bounded_queue.hpp"
#include "tenzir/async/grpc.hpp"
#include "tenzir/async/oneshot.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/variant.hpp"

#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>
#include <opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::accept_otlp::detail {

namespace collector_logs = ::opentelemetry::proto::collector::logs::v1;
namespace collector_metrics = ::opentelemetry::proto::collector::metrics::v1;
namespace collector_trace = ::opentelemetry::proto::collector::trace::v1;
namespace common = ::opentelemetry::proto::common::v1;

using namespace tenzir::si_literals;

constexpr auto default_max_message_size = uint64_t{64_Mi};
constexpr auto default_max_concurrent_requests = uint64_t{128};
constexpr auto max_inflight_request_bytes = uint64_t{512_Mi};
constexpr auto cancelled_error = std::string_view{"request was cancelled"};

enum class Signal {
  logs,
  metrics,
  traces,
};

enum class Encoding {
  protobuf,
  json,
};

enum class AttributeMode {
  list,
  record,
};

enum class Transport {
  http,
  grpc,
};

struct AcceptOtlpArgs {
  located<secret> endpoint;
  Option<located<std::string>> transport;
  Option<located<list>> signals;
  Option<located<std::string>> schema;
  Option<located<uint64_t>> max_message_size;
  Option<located<uint64_t>> max_concurrent_requests;
  Option<located<list>> include_metadata;
  Option<located<data>> tls;

  auto get_attribute_mode() const -> AttributeMode {
    return schema and schema->inner == "record" ? AttributeMode::record
                                                : AttributeMode::list;
  }

  auto get_transport() const -> Transport {
    return transport and transport->inner == "grpc" ? Transport::grpc
                                                    : Transport::http;
  }

  auto get_max_message_size() const -> size_t {
    return tenzir::detail::narrow<size_t>(
      max_message_size ? max_message_size->inner : default_max_message_size);
  }

  auto get_max_concurrent_requests() const -> uint64_t {
    auto const configured = max_concurrent_requests
                              ? max_concurrent_requests->inner
                              : default_max_concurrent_requests;
    // gRPC materializes a request before invoking its callback. HTTP can hold
    // one decoded body, one queued chunk, and one chunk in a blocked handler
    // per admitted request, so reserve three complete messages for HTTP.
    auto const request_bytes
      = std::max(uint64_t{1}, static_cast<uint64_t>(get_max_message_size()));
    auto const representation_factor
      = get_transport() == Transport::http ? uint64_t{3} : uint64_t{1};
    auto const byte_limited
      = std::max(uint64_t{1}, max_inflight_request_bytes
                                / (request_bytes * representation_factor));
    return std::min(configured, byte_limited);
  }

  auto accepts(Signal signal) const -> bool {
    if (not signals) {
      return true;
    }
    auto name = std::string_view{};
    switch (signal) {
      case Signal::logs:
        name = "logs";
        break;
      case Signal::metrics:
        name = "metrics";
        break;
      case Signal::traces:
        name = "traces";
        break;
    }
    return std::ranges::any_of(signals->inner, [&](data const& value) {
      auto const* signal_name = try_as<std::string>(&value);
      return signal_name and *signal_name == name;
    });
  }
};

using DecodedSlice = Result<table_slice, std::string>;
using DecodedSlices = generator<DecodedSlice>;
using DecodeResult = Result<DecodedSlices, std::string>;
using GrpcRequest = variant<collector_logs::ExportLogsServiceRequest,
                            collector_metrics::ExportMetricsServiceRequest,
                            collector_trace::ExportTraceServiceRequest>;

struct RequestMetadata {
  std::string client_ip;
  std::vector<std::pair<std::string, std::string>> metadata;
  Signal signal;
  Encoding encoding;
  Transport transport;
};

struct HttpResponse {
  uint16_t status = 200;
  std::string content_type;
  std::string body;
};

using ResponseSignal = Oneshot<HttpResponse>;

struct RequestStarted {
  uint64_t request_id;
  RequestMetadata metadata;
  bool gzip;
  Arc<ResponseSignal> response_signal;
};

struct RequestBody {
  uint64_t request_id;
  chunk_ptr chunk;
};

struct RequestFinished {
  uint64_t request_id;
  bool aborted;
};

struct GrpcRequestReceived {
  RequestMetadata metadata;
  GrpcRequest request;
  Arc<GrpcServerCall> call;
};

struct DrainTimeout {};
struct GrpcServerStopped {};
struct HttpResponsePending {};
struct HttpResponseDelivered {};
struct Noop {};

using Message
  = variant<Noop, DrainTimeout, GrpcServerStopped, HttpResponsePending,
            HttpResponseDelivered, RequestStarted, RequestBody, RequestFinished,
            GrpcRequestReceived>;
using MessageQueue = BoundedQueue<Message>;

struct DecodeContext {
  AttributeMode attribute_mode;
  ip peer_ip;
  list metadata;
  Transport transport;
  size_t receiver_metadata_size;
  std::function<bool()> cancellation_requested;
  std::function<void(std::string_view, common::AnyValue const&,
                     common::AnyValue const&)>
    warn_duplicate_attribute;
  mutable size_t duplicate_attribute_warnings_emitted = 0;

  auto is_cancelled() const -> bool {
    return cancellation_requested and cancellation_requested();
  }

  auto warn_about_duplicate_attribute(std::string_view key,
                                      common::AnyValue const& discarded,
                                      common::AnyValue const& kept) const
    -> void {
    constexpr auto warning_limit = size_t{16};
    if (duplicate_attribute_warnings_emitted >= warning_limit
        or not warn_duplicate_attribute) {
      return;
    }
    ++duplicate_attribute_warnings_emitted;
    warn_duplicate_attribute(key, discarded, kept);
  }
};

} // namespace tenzir::plugins::accept_otlp::detail
