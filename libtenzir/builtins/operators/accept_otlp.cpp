//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "accept_otlp/decode.hpp"
#include "accept_otlp/grpc.hpp"
#include "accept_otlp/http.hpp"
#include "accept_otlp/types.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tls_options.hpp"

#include <grpcpp/support/status.h>

#include <chrono>
#include <limits>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace tenzir::plugins::accept_otlp::detail {

namespace {

auto describe_attribute_value(common::AnyValue const& value) -> std::string {
  constexpr auto string_limit = size_t{128};
  if (value.has_string_value()) {
    auto const& string = value.string_value();
    auto result = fmt::format("{:?}", string.substr(0, string_limit));
    if (string.size() > string_limit) {
      result += "…";
    }
    return result;
  }
  if (value.has_bool_value()) {
    return fmt::format("{}", value.bool_value());
  }
  if (value.has_int_value()) {
    return fmt::format("{}", value.int_value());
  }
  if (value.has_double_value()) {
    return fmt::format("{}", value.double_value());
  }
  if (value.has_bytes_value()) {
    return fmt::format("<{} bytes>", value.bytes_value().size());
  }
  if (value.has_array_value()) {
    return fmt::format("<array with {} values>",
                       value.array_value().values_size());
  }
  if (value.has_kvlist_value()) {
    return fmt::format("<key-value list with {} entries>",
                       value.kvlist_value().values_size());
  }
  return "null";
}

auto warn_about_duplicate_attribute(std::string_view key,
                                    common::AnyValue const& discarded,
                                    common::AnyValue const& kept,
                                    location source, OpCtx& ctx) -> void {
  auto const discarded_value = describe_attribute_value(discarded);
  auto const kept_value = describe_attribute_value(kept);
  diagnostic::warning("duplicate OTLP attribute key `{}`; overwriting {} with "
                      "{}",
                      key, discarded_value, kept_value)
    .primary(source)
    .note("discarded value: {}", discarded_value)
    .note("new value: {}", kept_value)
    .emit(ctx);
}

} // namespace

class AcceptOtlp final : public Operator<void, table_slice> {
public:
  explicit AcceptOtlp(AcceptOtlpArgs args)
    : args_{std::move(args)},
      message_queue_{std::in_place, tenzir::detail::narrow<uint32_t>(
                                      args_.get_max_concurrent_requests())},
      active_requests_limit_{std::in_place,
                             args_.get_max_concurrent_requests()} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.get_transport() == Transport::grpc) {
      grpc_server_ = co_await OtlpGrpcServer::start(
        args_, message_queue_, active_requests_limit_, ctx);
      if (not grpc_server_) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
    } else {
      http_server_ = co_await start_http_server(args_, message_queue_,
                                                active_requests_limit_, ctx);
      if (not http_server_) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
    }
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_otlp"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_otlp"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    lifecycle_ = Lifecycle::running;
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
      [&](RequestStarted msg) -> Task<void> {
        auto decode_ctx = make_decode_context(msg.metadata, args_);
        if (decode_ctx.is_err()) {
          diagnostic::warning("rejected OTLP request: {}",
                              std::move(decode_ctx).unwrap_err())
            .primary(args_.endpoint)
            .emit(ctx);
          msg.response_signal->send(make_error_response(400));
          co_return;
        }
        auto decompressor
          = Option<std::shared_ptr<arrow::util::Decompressor>>{None{}};
        if (msg.gzip) {
          decompressor = http::make_decompressor("gzip", ctx);
          if (not decompressor) {
            msg.response_signal->send(make_error_response(400));
            co_return;
          }
        }
        auto context = std::move(decode_ctx).unwrap();
        context.warn_duplicate_attribute
          = [source = args_.endpoint.source,
             &ctx](std::string_view key, common::AnyValue const& discarded,
                   common::AnyValue const& kept) {
              warn_about_duplicate_attribute(key, discarded, kept, source, ctx);
            };
        active_requests_.emplace(
          msg.request_id,
          ActiveRequest{.metadata = std::move(msg.metadata),
                        .decode_ctx = std::move(context),
                        .decompressor = std::move(decompressor),
                        .response_signal = std::move(msg.response_signal),
                        .body = {},
                        .decompression_finished = false,
                        .rejected = false});
      },
      [&](RequestBody msg) -> Task<void> {
        auto it = active_requests_.find(msg.request_id);
        if (it == active_requests_.end() or it->second.rejected) {
          co_return;
        }
        auto& request = it->second;
        auto bytes = std::span<std::byte const>{
          reinterpret_cast<std::byte const*>(msg.chunk->data()),
          msg.chunk->size()};
        if (request.decompressor) {
          auto const remaining
            = args_.get_max_message_size() - request.body.size();
          auto output = http::decompress_chunk_with_status(
            **request.decompressor, bytes, ctx.dh(), remaining);
          if (output.is_err()) {
            request.rejected = true;
            request.response_signal->send(
              make_error_response(std::move(output).unwrap_err()));
            co_return;
          }
          auto decoded = std::move(output).unwrap();
          request.decompression_finished = decoded.finished;
          request.body.insert(request.body.end(), decoded.bytes.begin(),
                              decoded.bytes.end());
          bytes_read_counter_.add(decoded.bytes.size());
          co_return;
        }
        if (request.body.size() + bytes.size() > args_.get_max_message_size()) {
          request.rejected = true;
          request.response_signal->send(make_error_response(413));
          co_return;
        }
        request.body.insert(request.body.end(), bytes.begin(), bytes.end());
        bytes_read_counter_.add(bytes.size());
      },
      [&](RequestFinished msg) -> Task<void> {
        auto it = active_requests_.find(msg.request_id);
        if (it == active_requests_.end()) {
          co_return;
        }
        auto request = std::move(it->second);
        active_requests_.erase(it);
        if (msg.aborted or request.rejected) {
          request.response_signal->send(make_error_response(400));
          co_return;
        }
        if (request.decompressor and not request.decompression_finished) {
          diagnostic::warning("rejected incomplete gzip request")
            .primary(args_.endpoint)
            .emit(ctx);
          request.response_signal->send(make_error_response(400));
          co_return;
        }
        auto slices = decode(request.metadata.signal, request.metadata.encoding,
                             request.body, std::move(request.decode_ctx));
        if (slices.is_err()) {
          diagnostic::warning("rejected invalid OTLP request: {}",
                              std::move(slices).unwrap_err())
            .primary(args_.endpoint)
            .emit(ctx);
          request.response_signal->send(make_error_response(400));
          co_return;
        }
        for (auto&& slice : std::move(slices).unwrap()) {
          if (slice.is_err()) {
            diagnostic::warning("aborted OTLP request: {}",
                                std::move(slice).unwrap_err())
              .primary(args_.endpoint)
              .emit(ctx);
            request.response_signal->send(make_error_response(400));
            co_return;
          }
          auto materialized = std::move(slice).unwrap();
          auto const rows = materialized.rows();
          co_await push(std::move(materialized));
          events_read_counter_.add(rows);
        }
        request.response_signal->send(
          make_success_response(request.metadata.encoding));
      },
      [&](GrpcRequestReceived msg) -> Task<void> {
        if (msg.call->finished()) {
          co_return;
        }
        auto decode_ctx = make_decode_context(msg.metadata, args_);
        if (decode_ctx.is_err()) {
          auto error = std::move(decode_ctx).unwrap_err();
          diagnostic::warning("rejected OTLP/gRPC request: {}", error)
            .primary(args_.endpoint)
            .emit(ctx);
          std::ignore = msg.call->finish(
            grpc::Status{grpc::StatusCode::INTERNAL, std::move(error)});
          co_return;
        }
        bytes_read_counter_.add(match(msg.request, [](auto const& request) {
          return request.ByteSizeLong();
        }));
        auto grpc_decode_ctx = std::move(decode_ctx).unwrap();
        grpc_decode_ctx.cancellation_requested = [call = msg.call] {
          return call->finished();
        };
        grpc_decode_ctx.warn_duplicate_attribute
          = [source = args_.endpoint.source,
             &ctx](std::string_view key, common::AnyValue const& discarded,
                   common::AnyValue const& kept) {
              warn_about_duplicate_attribute(key, discarded, kept, source, ctx);
            };
        auto slices
          = decode(std::move(msg.request), std::move(grpc_decode_ctx));
        if (slices.is_err()) {
          if (msg.call->finished()) {
            co_return;
          }
          auto error = std::move(slices).unwrap_err();
          diagnostic::warning("rejected invalid OTLP/gRPC request: {}", error)
            .primary(args_.endpoint)
            .emit(ctx);
          std::ignore = msg.call->finish(
            grpc::Status{grpc::StatusCode::INVALID_ARGUMENT, std::move(error)});
          co_return;
        }
        if (msg.call->finished()) {
          co_return;
        }
        for (auto&& slice : std::move(slices).unwrap()) {
          if (slice.is_err()) {
            if (msg.call->finished()) {
              co_return;
            }
            auto error = std::move(slice).unwrap_err();
            diagnostic::warning("aborted OTLP/gRPC request: {}", error)
              .primary(args_.endpoint)
              .emit(ctx);
            std::ignore = msg.call->finish(grpc::Status{
              grpc::StatusCode::INVALID_ARGUMENT, std::move(error)});
            co_return;
          }
          if (msg.call->finished()) {
            co_return;
          }
          auto materialized = std::move(slice).unwrap();
          auto const rows = materialized.rows();
          co_await push(std::move(materialized));
          events_read_counter_.add(rows);
        }
        std::ignore = msg.call->finish(grpc::Status::OK);
      },
      [&](DrainTimeout) -> Task<void> {
        maybe_finish_draining();
        co_return;
      },
      [&](GrpcServerStopped) -> Task<void> {
        grpc_server_ = None{};
        maybe_finish_draining();
        co_return;
      },
      [&](HttpResponsePending) -> Task<void> {
        ++pending_http_responses_;
        co_return;
      },
      [&](HttpResponseDelivered) -> Task<void> {
        TENZIR_ASSERT(pending_http_responses_ > 0);
        --pending_http_responses_;
        maybe_finish_draining();
        co_return;
      },
      [&](Noop) -> Task<void> {
        co_return;
      });
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    begin_draining(ctx);
    maybe_finish_draining();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    begin_draining(ctx);
    maybe_finish_draining();
    co_return;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
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

  struct ActiveRequest {
    RequestMetadata metadata;
    DecodeContext decode_ctx;
    Option<std::shared_ptr<arrow::util::Decompressor>> decompressor;
    Arc<ResponseSignal> response_signal;
    blob body;
    bool decompression_finished = false;
    bool rejected = false;
  };

  static constexpr auto drain_timeout = std::chrono::seconds{5};

  auto force_stop() -> void {
    if (lifecycle_ == Lifecycle::done) {
      return;
    }
    lifecycle_ = Lifecycle::done;
    drain_deadline_ = None{};
    if (http_server_) {
      (*http_server_)->force_stop();
      http_server_ = None{};
    }
    if (grpc_server_) {
      (*grpc_server_)->shutdown(std::chrono::steady_clock::now());
      grpc_server_ = None{};
    }
  }

  auto begin_draining(OpCtx& ctx) -> void {
    if (lifecycle_ != Lifecycle::running) {
      return;
    }
    lifecycle_ = Lifecycle::draining;
    drain_deadline_ = std::chrono::steady_clock::now() + drain_timeout;
    ctx.spawn_task([queue = message_queue_,
                    deadline = *drain_deadline_]() mutable -> Task<void> {
      co_await sleep_until(deadline);
      queue->force_enqueue(DrainTimeout{});
    });
    if (http_server_) {
      (*http_server_)->drain();
    }
    if (grpc_server_) {
      (*grpc_server_)->shutdown(*drain_deadline_);
    }
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ != Lifecycle::draining) {
      return;
    }
    if (drain_deadline_
        and std::chrono::steady_clock::now() >= *drain_deadline_) {
      force_stop();
      return;
    }
    if (active_requests_limit_->available_permits()
          != tenzir::detail::narrow<size_t>(args_.get_max_concurrent_requests())
        or pending_http_responses_ > 0) {
      return;
    }
    if (grpc_server_) {
      return;
    }
    drain_deadline_ = None{};
    if (http_server_) {
      (*http_server_)->finish();
      http_server_ = None{};
    }
    lifecycle_ = Lifecycle::done;
  }

  AcceptOtlpArgs args_;
  mutable Arc<MessageQueue> message_queue_;
  Option<Box<OtlpHttpServer>> http_server_;
  Option<Box<OtlpGrpcServer>> grpc_server_;
  std::unordered_map<uint64_t, ActiveRequest> active_requests_;
  Arc<Semaphore> active_requests_limit_;
  size_t pending_http_responses_ = 0;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  Lifecycle lifecycle_ = Lifecycle::starting;
  Option<std::chrono::steady_clock::time_point> drain_deadline_ = None{};
};

class AcceptOtlpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "accept_otlp";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptOtlpArgs, AcceptOtlp>{};
    d.positional("endpoint", &AcceptOtlpArgs::endpoint);
    auto transport = d.named("transport", &AcceptOtlpArgs::transport);
    auto signals = d.named("signals", &AcceptOtlpArgs::signals, "list<string>");
    auto schema = d.named("schema", &AcceptOtlpArgs::schema);
    auto max_message_size
      = d.named("max_message_size", &AcceptOtlpArgs::max_message_size);
    auto max_concurrent_requests = d.named(
      "max_concurrent_requests", &AcceptOtlpArgs::max_concurrent_requests);
    auto include_metadata = d.named(
      "include_metadata", &AcceptOtlpArgs::include_metadata, "list<string>");
    auto tls_validator
      = tls_options{{.tls_default = false, .is_server = true}}.add_to_describer(
        d, &AcceptOtlpArgs::tls);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      if (auto value = ctx.get(transport);
          value and value->inner != "http" and value->inner != "grpc") {
        diagnostic::error("unsupported OTLP transport `{}`", value->inner)
          .primary(value->source)
          .note("supported transports are `http` and `grpc`")
          .emit(ctx);
      }
      if (auto value = ctx.get(signals)) {
        auto seen = std::unordered_set<std::string_view>{};
        if (value->inner.empty()) {
          diagnostic::error("`signals` must not be empty")
            .primary(value->source)
            .emit(ctx);
        }
        for (auto const& item : value->inner) {
          auto const* signal = try_as<std::string>(&item);
          if (not signal) {
            diagnostic::error("`signals` must be a list of strings")
              .primary(value->source)
              .emit(ctx);
          } else if (*signal != "logs" and *signal != "metrics"
                     and *signal != "traces") {
            diagnostic::error("unsupported OTLP signal `{}`", *signal)
              .primary(value->source)
              .note("supported signals are `logs`, `metrics`, and `traces`")
              .emit(ctx);
          } else if (not seen.emplace(*signal).second) {
            diagnostic::error("duplicate OTLP signal `{}`", *signal)
              .primary(value->source)
              .emit(ctx);
          }
        }
      }
      if (auto value = ctx.get(schema);
          value and value->inner != "list" and value->inner != "record") {
        diagnostic::error("unsupported output schema `{}`", value->inner)
          .primary(value->source)
          .note("supported schemas are `list` and `record`")
          .emit(ctx);
      }
      auto validate_size
        = [&](auto arg, std::string_view name, uint64_t maximum) {
            if (auto value = ctx.get(arg)) {
              if (value->inner == 0) {
                diagnostic::error("`{}` must be greater than 0", name)
                  .primary(value->source)
                  .emit(ctx);
              } else if (value->inner > maximum) {
                diagnostic::error("`{}` is too large", name)
                  .primary(value->source)
                  .note("maximum supported value: {}", maximum)
                  .emit(ctx);
              }
            }
          };
      validate_size(max_message_size, "max_message_size",
                    std::numeric_limits<int>::max());
      validate_size(max_concurrent_requests, "max_concurrent_requests",
                    std::numeric_limits<uint32_t>::max());
      if (auto value = ctx.get(include_metadata)) {
        auto seen = std::unordered_set<std::string>{};
        for (auto const& item : value->inner) {
          auto const* name = try_as<std::string>(&item);
          if (not name) {
            diagnostic::error("`include_metadata` must be a list of strings")
              .primary(value->source)
              .emit(ctx);
          } else if (name->empty()) {
            diagnostic::error("metadata names must not be empty")
              .primary(value->source)
              .emit(ctx);
          } else if (tenzir::detail::ascii_tolower(*name).ends_with("-bin")) {
            diagnostic::error("binary metadata is not supported")
              .primary(value->source)
              .note("remove metadata key `{}`", *name)
              .emit(ctx);
          } else if (not std::ranges::all_of(*name, [](unsigned char c) {
                       return std::isalnum(c) or c == '-' or c == '_'
                              or c == '.';
                     })) {
            diagnostic::error("invalid metadata name `{}`", *name)
              .primary(value->source)
              .note("metadata names may contain letters, digits, `-`, `_`, "
                    "and `.`")
              .emit(ctx);
          } else if (not seen.emplace(tenzir::detail::ascii_tolower(*name))
                           .second) {
            diagnostic::error("duplicate metadata name `{}`", *name)
              .primary(value->source)
              .emit(ctx);
          }
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::accept_otlp::detail

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_otlp::detail::AcceptOtlpPlugin)
