//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "from_velociraptor/common.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <fmt/ranges.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "velociraptor.grpc.pb.h"

// BEGIN Workaround https://github.com/abseil/abseil-cpp/issues/1747
#include <absl/base/config.h>
#include <absl/log/internal/log_message.h>

namespace absl {

ABSL_NAMESPACE_BEGIN

namespace log_internal {

template LogMessage& LogMessage::operator<<(const char& v);
template LogMessage& LogMessage::operator<<(const signed char& v);
template LogMessage& LogMessage::operator<<(const unsigned char& v);
template LogMessage& LogMessage::operator<<(const short& v);
template LogMessage& LogMessage::operator<<(const unsigned short& v);
template LogMessage& LogMessage::operator<<(const int& v);
template LogMessage& LogMessage::operator<<(const unsigned int& v);
template LogMessage& LogMessage::operator<<(const long& v);
template LogMessage& LogMessage::operator<<(const unsigned long& v);
template LogMessage& LogMessage::operator<<(const long long& v);
template LogMessage& LogMessage::operator<<(const unsigned long long& v);
template LogMessage& LogMessage::operator<<(void* const& v);
template LogMessage& LogMessage::operator<<(const void* const& v);
template LogMessage& LogMessage::operator<<(const float& v);
template LogMessage& LogMessage::operator<<(const double& v);
template LogMessage& LogMessage::operator<<(const bool& v);

} // namespace log_internal

ABSL_NAMESPACE_END

} // namespace absl

// END Workaround

namespace tenzir::plugins::velociraptor {

using namespace std::chrono_literals;

namespace {

class velociraptor_operator final
  : public crtp_operator<velociraptor_operator> {
public:
  velociraptor_operator() = default;

  velociraptor_operator(operator_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto client_config = make_client_config(config_, ctrl.diagnostics());
    if (not client_config) {
      co_return;
    }
    TENZIR_DEBUG("establishing gRPC channel to {}",
                 client_config->api_connection_string);
    auto credentials = grpc::SslCredentials({
      .pem_root_certs = client_config->ca_certificate,
      .pem_private_key = client_config->client_private_key,
      .pem_cert_chain = client_config->client_cert,
    });
    auto channel_args = grpc::ChannelArguments{};
    // Overriding the target name is necessary to connect by IP address
    // because Velociraptor uses self-signed certs.
    channel_args.SetSslTargetNameOverride("VelociraptorServer");
    auto channel = grpc::CreateCustomChannel(
      client_config->api_connection_string, credentials, channel_args);
    auto stub = proto::API::NewStub(channel);
    for (const auto& request : args_.requests) {
      TENZIR_DEBUG("staging request {}: {}", request.name, request.vql);
    }
    auto args = make_collector_args(args_);
    TENZIR_DEBUG("submitting request: max_row = {}, max_wait = {}, org_id = "
                 "{}",
                 args_.max_rows, args_.max_wait, args_.org_id);
    auto context = grpc::ClientContext{};
    auto completion_queue = grpc::CompletionQueue{};
    auto reader = stub->AsyncQuery(&context, args, &completion_queue, nullptr);
    auto cleanup_guard = detail::scope_guard{[&]() noexcept {
      auto status = grpc::Status{};
      reader->Finish(&status, nullptr);
      // Shut down the completion queue to signal no more operations.
      completion_queue.Shutdown();
      // Drain remaining events including the Finish event.
      void* tag = nullptr;
      bool ok = false;
      while (completion_queue.Next(&tag, &ok)) {
        // Continue draining until all events are processed.
      }
      (void)status;
      (void)ok;
    }};
    auto done = false;
    auto read = true;
    auto response = proto::VQLResponse{};
    auto input_tag = uint64_t{0};
    co_yield {};
    while (not done) {
      TENZIR_DEBUG("reading response");
      if (read) {
        ++input_tag;
        reader->Read(&response, reinterpret_cast<void*>(input_tag));
        read = false;
      }
      auto deadline = std::chrono::system_clock::now() + 250ms;
      auto output_tag = uint64_t{0};
      auto ok = false;
      auto result = completion_queue.AsyncNext(
        reinterpret_cast<void**>(&output_tag), &ok, deadline);
      switch (result) {
        case grpc::CompletionQueue::SHUTDOWN: {
          TENZIR_DEBUG("drained completion queue");
          TENZIR_ASSERT(not ok);
          done = true;
          break;
        }
        case grpc::CompletionQueue::GOT_EVENT: {
          TENZIR_DEBUG("got event #{} (ok = {})", output_tag, ok);
          if (ok) {
            if (output_tag == input_tag) {
              if (auto slices = parse(response)) {
                for (const auto& slice : *slices) {
                  co_yield slice;
                }
              } else {
                emit_parse_warning(response, ctrl.diagnostics(),
                                   slices.error());
              }
              read = true;
            }
          } else {
            // When `ok` is false, future calls to Next() will never return true
            // again, so we can exit our loop.
            done = true;
          }
          break;
        }
        case grpc::CompletionQueue::TIMEOUT: {
          TENZIR_ASSERT(not ok);
          co_yield {};
          break;
        }
      }
    }
    cleanup_guard.trigger();
  }

  auto name() const -> std::string override {
    return "velociraptor";
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, velociraptor_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_;
  record config_;
};

class plugin final : public operator_plugin<velociraptor_operator>,
                     public virtual operator_factory_plugin {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `velociraptor.yaml` "
                               "instead",
                               this->name())
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

  auto signature() const -> operator_signature override {
    return {
      .source = true,
    };
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = operator_args{};
    auto org_id = std::optional<located<std::string>>{};
    auto request_name = std::optional<located<std::string>>{};
    auto max_rows = std::optional<located<uint64_t>>{};
    auto subscribe = std::optional<located<std::string>>{};
    auto max_wait = std::optional<located<duration>>{};
    auto query = std::optional<located<std::string>>{};
    auto profile = std::optional<located<std::string>>{};
    argument_parser2::operator_("from_velociraptor")
      .named("request_name", request_name)
      .named("org_id", org_id)
      .named("query", query)
      .named("max_rows", max_rows)
      .named("subscribe", subscribe)
      .named("max_wait", max_wait)
      .named("profile", profile)
      .parse(inv, ctx)
      .ignore();
    if (max_wait and max_wait->inner < 1s) {
      diagnostic::error("`max_wait` too low")
        .primary(max_wait->source)
        .hint("value must be great than 1s")
        .emit(ctx);
      return failure::promise();
    }
    if (query) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = std::move(query->inner),
      });
    }
    if (subscribe) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = make_subscribe_query(subscribe->inner),
      });
    }
    if (args.requests.empty()) {
      diagnostic::error("no artifact subscription or VQL expression provided")
        .hint("specify `subscribe=<artifact>` for a subscription")
        .hint("specify `query=<vql>` to run a VQL expression")
        .emit(ctx);
      return failure::promise();
    }
    args.org_id = org_id ? org_id->inner : default_org_id;
    args.max_rows = max_rows ? max_rows->inner : default_max_rows;
    args.max_wait = std::chrono::duration_cast<std::chrono::seconds>(
      max_wait ? max_wait->inner : default_max_wait);
    auto profiles = available_profiles(config_);
    if (profile) {
      if (profiles.empty()) {
        diagnostic::error("no profiles configured")
          .primary(profile->source)
          .emit(ctx);
        return failure::promise();
      }
      auto profile_config = try_get_only<record>(
        config_, fmt::format("profiles.{}", profile->inner));
      if (not profile_config) {
        diagnostic::error("profile `{}` is invalid: {}", profile->inner,
                          profile_config.error())
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(profiles, ", "))
          .emit(ctx);
        return failure::promise();
      }
      if (not *profile_config) {
        diagnostic::error("profile `{}` does not exist", profile->inner)
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(profiles, ", "))
          .emit(ctx);
        return failure::promise();
      }
      return std::make_unique<velociraptor_operator>(std::move(args),
                                                     **profile_config);
    }
    if (profiles.empty()) {
      return std::make_unique<velociraptor_operator>(std::move(args), config_);
    }
    // If we have profiles configured but no --profile set, we default to the
    // first configured profile.
    auto profile_config = try_get_only<record>(
      config_, fmt::format("profiles.{}", profiles.front()));
    if (not profile_config or not *profile_config) {
      diagnostic::error("profile `{}` is invalid", profiles.front())
        .note("implicitly used the first configured profile")
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<velociraptor_operator>(std::move(args),
                                                   **profile_config);
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    auto parser = argument_parser{name(), "https://docs.tenzir.com/operators/"
                                          "velociraptor"};
    auto org_id = std::optional<located<std::string>>{};
    auto request_name = std::optional<located<std::string>>{};
    auto max_rows = std::optional<located<uint64_t>>{};
    auto subscribe = std::optional<located<std::string>>{};
    auto max_wait = std::optional<located<duration>>{};
    auto query = std::optional<located<std::string>>{};
    auto profile = std::optional<located<std::string>>{};
    parser.add("-n,--request-name", request_name, "<string>");
    parser.add("-o,--org-id", org_id, "<string>");
    parser.add("-q,--query", query, "<vql>");
    parser.add("-r,--max-rows", max_rows, "<uint64>");
    parser.add("-s,--subscribe", subscribe, "<artifact>");
    parser.add("-w,--max-wait", max_wait, "<duration>");
    parser.add("--profile", profile, "<profile>");
    parser.parse(p);
    if (max_wait and max_wait->inner < 1s) {
      diagnostic::error("--max-wait too low")
        .primary(max_wait->source)
        .hint("value must be great than 1s")
        .throw_();
    }
    if (query) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = std::move(query->inner),
      });
    }
    if (subscribe) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = make_subscribe_query(subscribe->inner),
      });
    }
    if (args.requests.empty()) {
      diagnostic::error("no artifact subscription or VQL expression provided")
        .hint("use -s,--subscribe <artifact> for a subscription")
        .hint("use -q,--query <vql> to run a VQL expression")
        .throw_();
    }
    args.org_id = org_id ? org_id->inner : default_org_id;
    args.max_rows = max_rows ? max_rows->inner : default_max_rows;
    args.max_wait = std::chrono::duration_cast<std::chrono::seconds>(
      max_wait ? max_wait->inner : default_max_wait);
    auto profiles = available_profiles(config_);
    if (profile) {
      if (profiles.empty()) {
        diagnostic::error("no profiles configured")
          .primary(profile->source)
          .throw_();
      }
      auto profile_config = try_get_only<record>(
        config_, fmt::format("profiles.{}", profile->inner));
      if (not profile_config) {
        diagnostic::error("profile `{}` is invalid: {}", profile->inner,
                          profile_config.error())
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(profiles, ", "))
          .throw_();
      }
      if (not *profile_config) {
        diagnostic::error("profile `{}` does not exist", profile->inner)
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(profiles, ", "))
          .throw_();
      }
      return std::make_unique<velociraptor_operator>(std::move(args),
                                                     **profile_config);
    }
    if (profiles.empty()) {
      return std::make_unique<velociraptor_operator>(std::move(args), config_);
    }
    // If we have profiles configured but no --profile set, we default to the
    // first configured profile.
    auto profile_config = try_get_only<record>(
      config_, fmt::format("profiles.{}", profiles.front()));
    if (not profile_config or not *profile_config) {
      diagnostic::error("profile `{}` is invalid", profiles.front())
        .note("implicitly used the first configured profile")
        .throw_();
    }
    return std::make_unique<velociraptor_operator>(std::move(args),
                                                   **profile_config);
  }

  auto name() const -> std::string override {
    return "from_velociraptor";
  }

  auto operator_name() const -> std::string override {
    return "velociraptor";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::velociraptor

TENZIR_REGISTER_PLUGIN(tenzir::plugins::velociraptor::plugin)
