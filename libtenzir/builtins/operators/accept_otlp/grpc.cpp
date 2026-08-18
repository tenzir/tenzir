//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "grpc.hpp"

#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/async/grpc.hpp"
#include "tenzir/detail/grpc.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http_server.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/try.hpp"

#include <grpcpp/resource_quota.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h>
#include <opentelemetry/proto/collector/metrics/v1/metrics_service.grpc.pb.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h>

#include <algorithm>
#include <memory>

namespace tenzir::plugins::accept_otlp::detail {

namespace {

auto grpc_request_metadata(grpc::CallbackServerContext const& context,
                           Signal signal)
  -> Result<RequestMetadata, std::string> {
  auto peer_ip = tenzir::detail::grpc_peer_ip(context.peer());
  if (peer_ip.is_err()) {
    return Err{std::move(peer_ip).unwrap_err()};
  }
  auto metadata = std::vector<std::pair<std::string, std::string>>{};
  metadata.reserve(context.client_metadata().size());
  for (auto const& [key, value] : context.client_metadata()) {
    auto name = std::string{key.data(), key.size()};
    if (name.ends_with("-bin")) {
      continue;
    }
    metadata.emplace_back(tenzir::detail::ascii_tolower(name),
                          std::string{value.data(), value.size()});
  }
  return RequestMetadata{.client_ip = std::move(peer_ip).unwrap(),
                         .metadata = std::move(metadata),
                         .signal = signal,
                         .encoding = Encoding::protobuf,
                         .transport = Transport::grpc};
}

template <class Request>
auto enqueue_grpc_request(grpc::CallbackServerContext& context,
                          Request const& request, Signal signal,
                          Arc<MessageQueue> queue,
                          Arc<Semaphore> active_requests_limit)
  -> grpc::ServerUnaryReactor* {
  auto handle = GrpcServerCall::make(context, std::move(active_requests_limit));
  if (not handle.admitted) {
    std::ignore = handle.call->finish(grpc::Status{
      grpc::StatusCode::UNAVAILABLE, "OTLP receiver is overloaded"});
    return handle.reactor;
  }
  auto metadata = grpc_request_metadata(context, signal);
  if (metadata.is_err()) {
    std::ignore = handle.call->finish(grpc::Status{
      grpc::StatusCode::INVALID_ARGUMENT, std::move(metadata).unwrap_err()});
    return handle.reactor;
  }
  // Cancellation may release gRPC's request storage before the async operator
  // task observes this message, so the callback must enqueue an owned copy.
  auto message = GrpcRequestReceived{
    .metadata = std::move(metadata).unwrap(),
    .request = GrpcRequest{request},
    .call = handle.call,
  };
  if (not queue->try_enqueue(Message{std::move(message)})) {
    std::ignore = handle.call->finish(grpc::Status{
      grpc::StatusCode::UNAVAILABLE, "OTLP receiver is overloaded"});
  }
  return handle.reactor;
}

class LogsGrpcService final
  : public collector_logs::LogsService::CallbackService {
public:
  LogsGrpcService(Arc<MessageQueue> queue, Arc<Semaphore> active_requests_limit)
    : queue_{std::move(queue)},
      active_requests_limit_{std::move(active_requests_limit)} {
  }

  auto Export(grpc::CallbackServerContext* context,
              collector_logs::ExportLogsServiceRequest const* request,
              collector_logs::ExportLogsServiceResponse*)
    -> grpc::ServerUnaryReactor* override {
    return enqueue_grpc_request(*context, *request, Signal::logs, queue_,
                                active_requests_limit_);
  }

private:
  Arc<MessageQueue> queue_;
  Arc<Semaphore> active_requests_limit_;
};

class MetricsGrpcService final
  : public collector_metrics::MetricsService::CallbackService {
public:
  MetricsGrpcService(Arc<MessageQueue> queue,
                     Arc<Semaphore> active_requests_limit)
    : queue_{std::move(queue)},
      active_requests_limit_{std::move(active_requests_limit)} {
  }

  auto Export(grpc::CallbackServerContext* context,
              collector_metrics::ExportMetricsServiceRequest const* request,
              collector_metrics::ExportMetricsServiceResponse*)
    -> grpc::ServerUnaryReactor* override {
    return enqueue_grpc_request(*context, *request, Signal::metrics, queue_,
                                active_requests_limit_);
  }

private:
  Arc<MessageQueue> queue_;
  Arc<Semaphore> active_requests_limit_;
};

class TracesGrpcService final
  : public collector_trace::TraceService::CallbackService {
public:
  TracesGrpcService(Arc<MessageQueue> queue,
                    Arc<Semaphore> active_requests_limit)
    : queue_{std::move(queue)},
      active_requests_limit_{std::move(active_requests_limit)} {
  }

  auto Export(grpc::CallbackServerContext* context,
              collector_trace::ExportTraceServiceRequest const* request,
              collector_trace::ExportTraceServiceResponse*)
    -> grpc::ServerUnaryReactor* override {
    return enqueue_grpc_request(*context, *request, Signal::traces, queue_,
                                active_requests_limit_);
  }

private:
  Arc<MessageQueue> queue_;
  Arc<Semaphore> active_requests_limit_;
};

struct GrpcServices {
  GrpcServices(Arc<MessageQueue> queue, Arc<Semaphore> active_requests_limit)
    : queue_{std::move(queue)},
      logs_{queue_, active_requests_limit},
      metrics_{queue_, active_requests_limit},
      traces_{queue_, std::move(active_requests_limit)} {
  }

  Arc<MessageQueue> queue_;
  LogsGrpcService logs_;
  MetricsGrpcService metrics_;
  TracesGrpcService traces_;
};

struct StartedGrpcServer {
  Arc<GrpcServices> services;
  std::unique_ptr<grpc::Server> server;
};

} // namespace

struct OtlpGrpcServer::Impl {
  explicit Impl(Box<GrpcServerHandle> server) : server{std::move(server)} {
  }

  Box<GrpcServerHandle> server;
};

OtlpGrpcServer::OtlpGrpcServer(Box<Impl> impl) : impl_{std::move(impl)} {
}

OtlpGrpcServer::~OtlpGrpcServer() = default;

auto OtlpGrpcServer::start(AcceptOtlpArgs const& args, Arc<MessageQueue> queue,
                           Arc<Semaphore> active_requests_limit, OpCtx& ctx)
  -> Task<Option<Box<OtlpGrpcServer>>> {
  auto const& cfg = ctx.actor_system().config();
  auto endpoint = std::string{};
  auto requests = std::vector<secret_request>{
    make_secret_request("endpoint", args.endpoint, endpoint, ctx.dh())};
  if ((co_await ctx.resolve_secrets(std::move(requests))).is_error()) {
    co_return None{};
  }
  auto parsed
    = http_server::parse_endpoint(endpoint, args.endpoint.source, ctx.dh());
  if (not parsed) {
    co_return None{};
  }
  auto const tls_enabled = http_server::is_tls_enabled(args.tls, cfg);
  if (parsed->scheme_tls) {
    if (*parsed->scheme_tls and not tls_enabled) {
      diagnostic::error("`https://` endpoint requires `tls=true`")
        .primary(args.endpoint)
        .emit(ctx);
      co_return None{};
    }
    if (not *parsed->scheme_tls and tls_enabled) {
      diagnostic::error("`http://` endpoint requires `tls=false`")
        .primary(args.endpoint)
        .emit(ctx);
      co_return None{};
    }
  }
  auto options = tls_options::from_optional(args.tls, {.tls_default = false,
                                                       .is_server = true});
  auto tls = options.resolve(cfg, ctx);
  if (not tls) {
    co_return None{};
  }
  auto host = parsed->host;
  if (host.contains(':') and not host.starts_with('[')) {
    host = fmt::format("[{}]", host);
  }
  auto address = fmt::format("{}:{}", host, parsed->port);
  auto max_message_size
    = tenzir::detail::narrow<int>(args.get_max_message_size());
  auto resource_quota_size = tenzir::detail::narrow<size_t>(
    std::max(max_inflight_request_bytes,
             uint64_t{2} * static_cast<uint64_t>(max_message_size)));
  auto accept_logs = args.accepts(Signal::logs);
  auto accept_metrics = args.accepts(Signal::metrics);
  auto accept_traces = args.accepts(Signal::traces);
  auto started = co_await spawn_blocking(
    [queue = std::move(queue),
     active_requests_limit = std::move(active_requests_limit),
     address = std::move(address), tls = std::move(*tls),
     endpoint_location = args.endpoint.source, max_message_size,
     resource_quota_size, accept_logs, accept_metrics,
     accept_traces]() mutable -> Result<StartedGrpcServer, diagnostic> {
      TRY(auto credentials,
          tenzir::detail::make_grpc_server_credentials(tls, endpoint_location));
      auto services = Arc<GrpcServices>{std::in_place, std::move(queue),
                                        std::move(active_requests_limit)};
      auto resource_quota = grpc::ResourceQuota{};
      resource_quota.Resize(resource_quota_size);
      auto builder = grpc::ServerBuilder{};
      builder.SetResourceQuota(resource_quota);
      builder.SetMaxReceiveMessageSize(max_message_size);
      auto selected_port = 0;
      builder.AddListeningPort(address, std::move(credentials), &selected_port);
      if (accept_logs) {
        builder.RegisterService(&services->logs_);
      }
      if (accept_metrics) {
        builder.RegisterService(&services->metrics_);
      }
      if (accept_traces) {
        builder.RegisterService(&services->traces_);
      }
      auto server = builder.BuildAndStart();
      if (not server or selected_port == 0) {
        return Err{diagnostic::error(
                     "failed to start OTLP/gRPC server: failed to bind gRPC "
                     "listener")
                     .primary(endpoint_location)
                     .done()};
      }
      return StartedGrpcServer{.services = std::move(services),
                               .server = std::move(server)};
    });
  if (started.is_err()) {
    ctx.dh().emit(std::move(started).unwrap_err());
    co_return None{};
  }
  auto started_server = std::move(started).unwrap();
  auto services = std::move(started_server.services);
  auto server = Box<GrpcServerHandle>{
    std::in_place, std::move(started_server.server),
    [services = std::move(services)]() mutable {
      services->queue_->force_enqueue(GrpcServerStopped{});
    }};
  auto impl = Box<Impl>{std::in_place, std::move(server)};
  auto result
    = std::unique_ptr<OtlpGrpcServer>{new OtlpGrpcServer{std::move(impl)}};
  co_return Box<OtlpGrpcServer>::from_non_null(std::move(result));
}

auto OtlpGrpcServer::shutdown(std::chrono::steady_clock::time_point deadline)
  -> void {
  impl_->server->shutdown(deadline);
}

} // namespace tenzir::plugins::accept_otlp::detail
