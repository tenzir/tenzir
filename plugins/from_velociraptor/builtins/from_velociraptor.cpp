//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "from_velociraptor/common.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/async.hpp>
#include <tenzir/box.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/variant.hpp>

#include <fmt/ranges.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/client_callback.h>

#include <atomic>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "velociraptor.grpc.pb.h"

namespace tenzir::plugins::velociraptor {

using namespace std::chrono_literals;

namespace {

constexpr auto message_queue_capacity = uint32_t{1024};
constexpr auto reactor_queue_overflow_error
  = "Velociraptor reactor queue overflow";
constexpr auto grpc_request_failed_error = "Velociraptor gRPC request failed";

struct FromVelociraptorArgs {
  record plugin_config;
  Option<located<std::string>> request_name;
  Option<located<std::string>> org_id;
  Option<located<uint64_t>> max_rows;
  Option<located<std::string>> subscribe;
  Option<located<duration>> max_wait;
  Option<located<std::string>> query;
  Option<located<std::string>> profile;
  location operator_location;
};

struct Response {
  proto::VQLResponse response;
};

struct StreamFinished {
  Option<std::string> error;
};

using Message = variant<Response, StreamFinished>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

auto normalize_args(FromVelociraptorArgs const& args, diagnostic_handler& dh)
  -> failure_or<operator_args> {
  auto result = operator_args{
    .max_rows = args.max_rows ? args.max_rows->inner : default_max_rows,
    .max_wait
    = args.max_wait
        ? std::chrono::duration_cast<std::chrono::seconds>(args.max_wait->inner)
        : default_max_wait,
    .org_id = args.org_id ? args.org_id->inner : default_org_id,
    .requests = {},
  };
  if (args.query) {
    result.requests.push_back(request{
      .name = args.request_name ? args.request_name->inner
                                : fmt::to_string(uuid::random()),
      .vql = args.query->inner,
    });
  }
  if (args.subscribe) {
    result.requests.push_back(request{
      .name = args.request_name ? args.request_name->inner
                                : fmt::to_string(uuid::random()),
      .vql = make_subscribe_query(args.subscribe->inner),
    });
  }
  if (result.max_wait < std::chrono::seconds{1}) {
    auto diag = diagnostic::error("`max_wait` too low");
    if (args.max_wait) {
      std::move(diag).primary(args.max_wait->source).emit(dh);
    } else {
      std::move(diag).primary(args.operator_location).emit(dh);
    }
    return failure::promise();
  }
  if (result.requests.empty()) {
    diagnostic::error("no artifact subscription or VQL expression provided")
      .primary(args.operator_location)
      .hint("specify `subscribe=<artifact>` for a subscription")
      .hint("specify `query=<vql>` to run a VQL expression")
      .emit(dh);
    return failure::promise();
  }
  return result;
}

auto select_client_config(FromVelociraptorArgs const& args,
                          diagnostic_handler& dh)
  -> failure_or<GrpcClientConfig> {
  auto profiles = available_profiles(args.plugin_config);
  if (args.profile) {
    if (profiles.empty()) {
      diagnostic::error("no profiles configured")
        .primary(args.profile->source)
        .emit(dh);
      return failure::promise();
    }
    auto profile_config = try_get_only<record>(
      args.plugin_config, fmt::format("profiles.{}", args.profile->inner));
    if (not profile_config) {
      diagnostic::error("profile `{}` is invalid: {}", args.profile->inner,
                        profile_config.error())
        .primary(args.profile->source)
        .emit(dh);
      return failure::promise();
    }
    if (not *profile_config) {
      diagnostic::error("profile `{}` does not exist", args.profile->inner)
        .primary(args.profile->source)
        .emit(dh);
      return failure::promise();
    }
    return make_client_config(**profile_config, dh, args.operator_location);
  }
  if (not profiles.empty()) {
    auto const& implicit_profile = profiles.front();
    auto profile_config = try_get_only<record>(
      args.plugin_config, fmt::format("profiles.{}", implicit_profile));
    if (not profile_config) {
      diagnostic::error("profile `{}` is invalid: {}", implicit_profile,
                        profile_config.error())
        .primary(args.operator_location)
        .note("implicitly used the first configured profile")
        .emit(dh);
      return failure::promise();
    }
    if (not *profile_config) {
      diagnostic::error("profile `{}` does not exist", implicit_profile)
        .primary(args.operator_location)
        .note("implicitly used the first configured profile")
        .emit(dh);
      return failure::promise();
    }
    return make_client_config(**profile_config, dh, args.operator_location);
  }
  return make_client_config(args.plugin_config, dh, args.operator_location);
}

class VelociraptorReadReactor final
  : public grpc::ClientReadReactor<proto::VQLResponse> {
public:
  VelociraptorReadReactor(proto::API::Stub& stub,
                          proto::VQLCollectorArgs request,
                          Arc<MessageQueue> message_queue)
    : request_{std::move(request)},
      message_queue_{std::move(message_queue)},
      done_future_{done_promise_.get_future().share()} {
    auto* async = stub.async();
    TENZIR_ASSERT(async);
    async->Query(&context_, &request_, this);
    StartCall();
    StartRead(&read_buffer_);
  }

  VelociraptorReadReactor(VelociraptorReadReactor const&) = delete;
  VelociraptorReadReactor(VelociraptorReadReactor&&) = delete;
  auto operator=(VelociraptorReadReactor const&)
    -> VelociraptorReadReactor& = delete;
  auto operator=(VelociraptorReadReactor&&)
    -> VelociraptorReadReactor& = delete;

  ~VelociraptorReadReactor() noexcept override {
    request_shutdown();
    std::ignore = done_future_.wait_for(std::chrono::seconds{5});
  }

  auto request_shutdown() -> void {
    local_shutdown_requested_.store(true, std::memory_order_release);
    context_.TryCancel();
  }

  void OnReadDone(bool ok) override {
    if (not ok) {
      return;
    }
    if (not message_queue_->try_enqueue(Message{Response{read_buffer_}})) {
      queue_overflow_.store(true, std::memory_order_release);
      request_shutdown();
      return;
    }
    StartRead(&read_buffer_);
  }

  void OnDone(grpc::Status const& status) override {
    auto error = Option<std::string>{};
    if (queue_overflow_.load(std::memory_order_acquire)) {
      error = std::string{reactor_queue_overflow_error};
    } else if (not status.ok()) {
      auto const is_local_cancel
        = status.error_code() == grpc::StatusCode::CANCELLED
          and local_shutdown_requested_.load(std::memory_order_acquire);
      if (not is_local_cancel) {
        auto message = status.error_message();
        if (message.empty()) {
          message = grpc_request_failed_error;
        }
        error = std::move(message);
      }
    }
    folly::coro::blocking_wait(
      message_queue_->enqueue(Message{StreamFinished{std::move(error)}}));
    done_promise_.set_value();
  }

private:
  grpc::ClientContext context_;
  proto::VQLCollectorArgs request_;
  Arc<MessageQueue> message_queue_;
  proto::VQLResponse read_buffer_;
  std::promise<void> done_promise_;
  std::shared_future<void> done_future_;
  std::atomic<bool> local_shutdown_requested_{false};
  std::atomic<bool> queue_overflow_{false};
};

class FromVelociraptor final : public Operator<void, table_slice> {
public:
  explicit FromVelociraptor(FromVelociraptorArgs args)
    : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_velociraptor"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsType::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_velociraptor"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsType::events);
    auto normalized = normalize_args(args_, ctx);
    TENZIR_ASSERT(normalized);
    auto config = select_client_config(args_, ctx.dh());
    TENZIR_ASSERT(config);
    auto credentials = grpc::SslCredentials({
      .pem_root_certs = config->ca_certificate,
      .pem_private_key = config->client_private_key,
      .pem_cert_chain = config->client_cert,
    });
    auto channel_args = grpc::ChannelArguments{};
    channel_args.SetSslTargetNameOverride("VelociraptorServer");
    auto channel = grpc::CreateCustomChannel(config->api_connection_string,
                                             credentials, channel_args);
    auto stub = proto::API::NewStub(channel);
    reactor_.emplace(std::in_place, *stub, make_collector_args(*normalized),
                     message_queue_);
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    TENZIR_ASSERT(reactor_);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto responses = std::vector<proto::VQLResponse>{};
    auto terminal_error = Option<std::string>{};
    auto collect_message = [&](Message message) -> void {
      if (auto* response = try_as<Response>(&message)) {
        responses.push_back(std::move(response->response));
        return;
      }
      auto finish_message = as<StreamFinished>(std::move(message));
      terminal_error = std::move(finish_message.error);
      stream_finished_ = true;
    };
    collect_message(std::move(result).as<Message>());
    while (auto next = message_queue_->try_dequeue()) {
      collect_message(std::move(*next));
    }
    for (auto& response : responses) {
      const auto bytes = response.ByteSizeLong();
      if (bytes > 0) {
        bytes_read_counter_.add(bytes);
      }
      if (auto slices = parse(response)) {
        for (auto& slice : *slices) {
          auto const rows = slice.rows();
          co_await push(std::move(slice));
          events_read_counter_.add(rows);
        }
      } else {
        emit_parse_warning(response, ctx.dh(), slices.error(),
                           args_.operator_location);
      }
    }
    TENZIR_ASSERT(not stream_finished_ or message_queue_->empty());
    if (terminal_error) {
      diagnostic::error(grpc_request_failed_error)
        .primary(args_.operator_location)
        .note("{}", *terminal_error)
        .emit(ctx);
    }
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_ASSERT(reactor_);
    (*reactor_)->request_shutdown();
    co_return;
  }

  auto state() -> OperatorState override {
    return stream_finished_ ? OperatorState::done : OperatorState::normal;
  }

private:
  FromVelociraptorArgs args_;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Box<VelociraptorReadReactor>> reactor_;
  bool stream_finished_ = false;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
};

class FromVelociraptorPlugin final : public virtual OperatorPlugin {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `velociraptor.yaml` "
                               "instead",
                               this->name())
        .primary(location::unknown)
        .to_error();
    }
    auto c
      = try_get_only<tenzir::record>(global_config, "plugins.velociraptor");
    if (not c) {
      return c.error();
    }
    if (*c) {
      config_ = **c;
    }
    return caf::none;
  }

  auto name() const -> std::string override {
    return "tql2.from_velociraptor";
  }

  auto describe() const -> Description override {
    auto initial = FromVelociraptorArgs{};
    initial.plugin_config = config_;
    auto d
      = Describer<FromVelociraptorArgs, FromVelociraptor>{std::move(initial)};
    auto request_name
      = d.named("request_name", &FromVelociraptorArgs::request_name);
    auto org_id = d.named("org_id", &FromVelociraptorArgs::org_id);
    auto query = d.named("query", &FromVelociraptorArgs::query);
    auto max_rows = d.named("max_rows", &FromVelociraptorArgs::max_rows);
    auto subscribe = d.named("subscribe", &FromVelociraptorArgs::subscribe);
    auto max_wait = d.named("max_wait", &FromVelociraptorArgs::max_wait);
    auto profile = d.named("profile", &FromVelociraptorArgs::profile);
    d.named_optional("_config", &FromVelociraptorArgs::plugin_config);
    d.operator_location(&FromVelociraptorArgs::operator_location);
    d.validate([request_name, org_id, query, max_rows, subscribe, max_wait,
                profile, this](DescribeCtx& ctx) -> Empty {
      TENZIR_UNUSED(request_name, org_id, max_rows, profile);
      auto args = FromVelociraptorArgs{};
      args.plugin_config = config_;
      args.operator_location = ctx.operator_location();
      if (auto value = ctx.get(request_name)) {
        args.request_name = *value;
      }
      if (auto value = ctx.get(org_id)) {
        args.org_id = *value;
      }
      if (auto value = ctx.get(query)) {
        args.query = *value;
      }
      if (auto value = ctx.get(max_rows)) {
        args.max_rows = *value;
      }
      if (auto value = ctx.get(subscribe)) {
        args.subscribe = *value;
      }
      if (auto value = ctx.get(max_wait)) {
        args.max_wait = *value;
      }
      if (auto value = ctx.get(profile)) {
        args.profile = *value;
      }
      std::ignore = normalize_args(args, ctx);
      std::ignore = select_client_config(args, ctx);
      return {};
    });
    return d.without_optimize();
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::velociraptor

TENZIR_REGISTER_PLUGIN(tenzir::plugins::velociraptor::FromVelociraptorPlugin)
